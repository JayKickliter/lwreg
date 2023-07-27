use anyhow::{anyhow, Result};
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use flate2::read::GzDecoder;
use hextree::{disktree::DiskTree, Cell, HexTreeMap};
use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::PathBuf,
};

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

    /// Lookup target H3 cell
    Lookup {
        /// On disk HexTreeMap
        map: PathBuf,
        /// Target h3 index
        idx: String,
    },
}

impl Cli {
    fn run(self) -> Result<()> {
        match self {
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
                    let mut reader = GzDecoder::new(file);
                    while let Ok(entry) = reader.read_u64::<LE>() {
                        region_map.insert(Cell::try_from(entry)?, n as u8);
                    }
                }

                // Create an array of region names that we derive from
                // the input files base names.
                let region_name_lut: Vec<&str> =
                    inputs.iter().map(|(name, _)| name.as_ref()).collect();

                // Turn the HexTreeMap into a disktree at `out`.
                let mut disktree_file = File::create(out)?;
                region_map.to_disktree(&mut disktree_file)?;

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

                let (_, region_name_lut_idx) = disktree
                    .get::<u8>(cell)?
                    .ok_or_else(|| anyhow::anyhow!("no entry"))?;

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
