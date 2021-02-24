use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File};
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub id: u64,
    pub server_id: Uuid,
    pub host: String,
    pub port: u32,
    pub snapshot_interval: u64,
    pub snapshot_dir_path: PathBuf,
    pub snapshot_file_name: String,
    pub replication_interval: u64,
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
        } else if config.snapshot_file_name.is_empty() {
            error_msg = "snapshot_file_name cannot be empty";
        } else if config.snapshot_interval == 0 {
            error_msg = "snapshot_interval must be greater than 0";
        } else if config.replication_interval == 0 {
            error_msg = "replication_interval must be greater than 0";
        }
        if !error_msg.is_empty() {
            return Err(Error::new(ErrorKind::Other, error_msg));
        }
        if config.snapshot_dir_path.starts_with("~/") {
            let remaining_path = config
                .snapshot_dir_path
                .strip_prefix("~/")
                .expect("Failed to replace ~ with home dir");
            config.snapshot_dir_path = dirs::home_dir()
                .expect("Couldn't determine home dir")
                .join(remaining_path);
        }
        create_dir_all(&config.snapshot_dir_path)?;

        Ok(config)
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn snapshot_path(&self) -> PathBuf {
        self.snapshot_dir_path.join(&self.snapshot_file_name)
    }
}
