use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u32,
    pub metadata_url: String,
    pub server_id: Uuid,
    pub upload_path: PathBuf,
    pub ping_interval: u64,
}
impl ServerConfig {
    pub fn load_config<T: AsRef<Path>>(path: &T) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let mut config: Self = match serde_yaml::from_reader(file) {
            Ok(s) => s,
            Err(err) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("Couldn't deserialize config file: {}", err),
                ))
            }
        };
        let mut error_msg = "";
        if config.host.is_empty() {
            error_msg = "host cannot be empty";
        } else if config.metadata_url.is_empty() {
            error_msg = "metadata_url cannot be empty";
        } else if config.ping_interval == 0 {
            error_msg = "ping_interval cannot must be greater than 0";
        }
        if !error_msg.is_empty() {
            return Err(Error::new(ErrorKind::Other, error_msg));
        }
        if config.upload_path.starts_with("~/") {
            let remaining_path = config
                .upload_path
                .strip_prefix("~/")
                .expect("Failed to replace ~ with home dir");
            config.upload_path = dirs::home_dir()
                .expect("Couldn't determine home dir")
                .join(remaining_path);
        }
        create_dir_all(&config.upload_path)?;

        Ok(config)
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
