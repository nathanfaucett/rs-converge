use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
  #[allow(unused_mut)]
  let mut config = tonic_prost_build::configure();

  #[cfg(feature = "file-descriptor-set")]
  {
    use std::{env, path::PathBuf};

    config = config.file_descriptor_set_path(PathBuf::from(env::var("OUT_DIR")?).join("db.bin"));
  }

  config.compile_protos(&["proto/db.proto"], &["proto"])?;

  Ok(())
}
