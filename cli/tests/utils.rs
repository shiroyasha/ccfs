use std::fs::{remove_dir_all, remove_file};
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn create_config_file(
    meta_url: &str,
    temp_dir: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let config_file_path = temp_dir.join("config.yml");
    let mut config_file = File::create(&config_file_path).await?;
    config_file
        .write_all(format!("metadata-server-url: {}", meta_url).as_bytes())
        .await?;
    Ok(config_file_path)
}

pub struct Cleanup {
    paths: Vec<PathBuf>,
}

impl Cleanup {
    #[allow(dead_code)]
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        for path in &self.paths {
            if path.exists() {
                match path.is_dir() {
                    true => remove_dir_all(path).unwrap(),
                    false => remove_file(path).unwrap(),
                }
            }
        }
    }
}
