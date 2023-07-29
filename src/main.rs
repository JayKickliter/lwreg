#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use anyhow::{anyhow, Result};
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use flate2::read::GzDecoder;
use geojson::{Feature, FeatureCollection, GeoJson, JsonObject};
use h3o::{
    geom::{Geometry, ToCells},
    CellIndex, Resolution,
};
use hextree::{disktree::DiskTree, Cell, HexTreeMap};
use rayon::prelude::*;
use serde_json::Value;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::PathBuf,
    sync::mpsc,
    thread,
};
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Parser)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Helium DiskTreeMap multitool")]
enum Cli {
    /// Generate a disktree from source h3idz sets
    Generate {
        /// Output file
        out: PathBuf,
        /// Input h3idz files
        sets: Vec<PathBuf>,
    },

    /// Generate a disktree from source h3idz sets
    GenWorld {
        /// Resolution to use for h3 cells
        #[arg(default_value_t = Resolution::Seven, short, long)]
        resolution: Resolution,
        /// Output file
        out: PathBuf,
        /// Input h3idz files
        world: PathBuf,
    },

    /// Lookup target H3 cell
    Lookup {
        /// On disk HexTreeMap
        map: PathBuf,
        /// Target h3 index
        idx: String,
    },
}

fn to_cells(
    idx: u8,
    feature: Feature,
    resolution: Resolution,
) -> Result<(u8, JsonObject, Vec<CellIndex>)> {
    println!("generating {:?}", feature.properties);
    let start = std::time::Instant::now();
    let properties = feature
        .properties
        .ok_or_else(|| anyhow!("no properties for feature {idx}"))?;
    let geometry = feature
        .geometry
        .ok_or_else(|| anyhow!("feature {idx} missing geometry"))?;
    let geometry = Geometry::try_from(&geometry)?;
    let cells = geometry.to_cells(resolution).collect();
    println!("  generated {:?} in {:?}", properties, start.elapsed());
    Ok((idx, properties, cells))
}

fn dedup_cells(mut cells: Vec<CellIndex>) -> Result<Vec<CellIndex>> {
    cells.sort_unstable();
    cells.dedup();
    Ok(cells)
}

fn compact_cells(cells: Vec<CellIndex>) -> Result<Vec<CellIndex>> {
    let compacted = CellIndex::compact(cells)?;
    Ok(compacted.collect())
}

impl Cli {
    fn run(self) -> Result<()> {
        match self {
            Cli::GenWorld {
                resolution,
                out,
                world,
            } => {
                let mut disktree_file = File::create(out)?;
                let feature_collection = {
                    let geojson_file = File::open(world)?;
                    let geojson = GeoJson::from_reader(geojson_file)?;
                    FeatureCollection::try_from(geojson)?
                };

                let mut world_map: HexTreeMap<u8> = HexTreeMap::new();
                let mut property_lut: Vec<(u8, String)> = Vec::new();

                let (sender, rx) = mpsc::channel::<(u8, String, Vec<CellIndex>)>();

                let thread_handle = thread::spawn(move || {
                    feature_collection
                        .features
                        .into_par_iter()
                        .enumerate()
                        .try_for_each_with(
                            (sender.clone(), resolution),
                            |(sender, resolution), (lut_idx, feature)| {
                                fn work_fun(
                                    idx: usize,
                                    feature: Feature,
                                    res: Resolution,
                                    tx: &mut mpsc::Sender<(u8, String, Vec<CellIndex>)>,
                                ) -> Result<()> {
                                    let idx = u8::try_from(idx)?;
                                    let (_, properties, cells) = to_cells(idx, feature, res)?;
                                    let cells = dedup_cells(cells)?;
                                    let cells = compact_cells(cells)?;
                                    let properties = Value::Object(properties);
                                    tx.send((idx, properties.to_string(), cells))?;
                                    Ok(())
                                }
                                work_fun(lut_idx, feature, *resolution, sender)
                            },
                        )
                });

                while let Ok((lut_idx, properties, cells)) = rx.recv() {
                    property_lut.push((lut_idx, properties));
                    for cell in cells {
                        let cell = Cell::from_raw(cell.into())?;
                        world_map.insert(cell, lut_idx);
                    }
                }

                thread_handle
                    .join()
                    .map_err(|join_err| anyhow!("thread join {:?}", join_err))
                    .unwrap()?;

                world_map.to_disktree(&mut disktree_file, |wtr, &val| wtr.write_u8(val))?;
                property_lut.sort_by_key(|(lut_idx, _)| *lut_idx);
                let property_lut: Vec<String> = property_lut
                    .into_iter()
                    .map(|(_lut_idx, properties)| properties)
                    .collect();

                // Append country LuT to end of `out` and write
                // its position the end of the file.
                let property_lut_pos = disktree_file.seek(SeekFrom::End(0))?;
                bincode::serialize_into(&mut disktree_file, &property_lut)?;
                disktree_file.write_u64::<LE>(property_lut_pos)?;
            }

            Cli::Generate { out, sets } => {
                // [(Region, Input File), ..]
                let inputs = {
                    let mut inputs: Vec<(String, File)> = Vec::new();
                    for path in sets {
                        // Extract filename until the first '.'
                        let name = path
                            .file_name()
                            .ok_or_else(|| anyhow!("not a file path: {}", path.to_string_lossy()))?
                            .to_str()
                            .ok_or_else(|| {
                                anyhow!("bad chars in file name: {}", path.to_string_lossy())
                            })?
                            .chars()
                            .take_while(|&c| c != '.')
                            .collect::<String>();

                        let file = File::open(path)?;
                        inputs.push((name, file));
                    }
                    // Not necessary, but makes debugging easier
                    // when viewing region name LuT in a hex editor.
                    inputs.sort_by(|a, b| a.0.cmp(&b.0));
                    inputs
                };

                // Create a map of H3 cells. For values, instead of
                // duplicating region strings, or creating an enum, we
                // store the index into region-string LuT.
                let mut region_map: HexTreeMap<u8> = HexTreeMap::new();
                for (n, (_name, file)) in inputs.iter().enumerate() {
                    let mut rdr = GzDecoder::new(file);
                    while let Ok(entry) = rdr.read_u64::<LE>() {
                        region_map.insert(Cell::try_from(entry)?, n as u8);
                    }
                }
                // Create an array of region names that we derive from
                // the input files base names.
                let region_name_lut: Vec<&str> =
                    inputs.iter().map(|(name, _)| name.as_ref()).collect();

                // Turn the HexTreeMap into a disktree at `out`.
                let mut disktree_file = File::create(out)?;
                region_map.to_disktree(&mut disktree_file, |wtr, &val| wtr.write_u8(val))?;

                // Append region-name LuT to end of `out` and write
                // its position the end of the file.
                let region_name_lut_pos = disktree_file.seek(SeekFrom::End(0))?;
                bincode::serialize_into(&mut disktree_file, &region_name_lut)?;
                disktree_file.write_u64::<LE>(region_name_lut_pos)?;
            }

            Cli::Lookup { map, idx } => {
                let cell_idx = u64::from_str_radix(&idx, 16)?;
                let cell = Cell::try_from(cell_idx)?;

                let mut disktree_file = File::open(map)?;
                disktree_file.seek(SeekFrom::End(-(std::mem::size_of::<u64>() as i64)))?;
                let region_name_lut_pos = disktree_file.read_u64::<LE>()?;
                disktree_file.seek(SeekFrom::Start(region_name_lut_pos))?;
                let region_name_lut: Vec<String> = bincode::deserialize_from(&mut disktree_file)?;

                let mut disktree = DiskTree::from_reader(disktree_file)?;

                let (_, rdr) = disktree
                    .seek_to_cell(cell)?
                    .ok_or_else(|| anyhow::anyhow!("no entry"))?;
                let region_name_lut_idx = rdr.read_u8()?;

                let val = region_name_lut
                    .get(region_name_lut_idx as usize)
                    .ok_or_else(|| {
                        anyhow::anyhow!("no interned value for index {region_name_lut_idx}")
                    })?;

                println!("{val}");
            }
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run()
}
