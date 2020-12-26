#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rocket_contrib;
extern crate ccfs_commons;
extern crate dirs;
extern crate mut_static;
extern crate rocket_multipart_form_data;

use ccfs_commons::Chunk;
use mut_static::MutStatic;
use rocket::http::ContentType;
use rocket::response::Stream;
use rocket::Data;
use rocket_contrib::json::JsonValue;
use rocket_contrib::uuid::Uuid as UuidRC;
use rocket_multipart_form_data::{
    MultipartFormData, MultipartFormDataField, MultipartFormDataOptions, RawField, TextField,
};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::path::PathBuf;
use std::{thread, time};
use uuid::Uuid;

const METADATA_URL_KEY: &str = "METADATA_URL";
const SERVER_ADDRESS_KEY: &str = "SERVER_ADDRESS";

lazy_static! {
    // should be replaced with DB
    static ref CHUNKS: MutStatic<HashMap<Uuid, Chunk>> = MutStatic::new();
    static ref UPLOADS_DIR: PathBuf = {
      let mut path_buf = dirs::home_dir().unwrap();
      path_buf.push("ccfs-uploads");
      path_buf
    };
    static ref ID: Uuid = Uuid::new_v4();
}

#[post("/upload", data = "<data>")]
fn multipart_upload(content_type: &ContentType, data: Data) -> JsonValue {
    if !UPLOADS_DIR.exists() {
        fs::create_dir(UPLOADS_DIR.as_path()).unwrap();
    }
    let mut options = MultipartFormDataOptions::new();
    options.temporary_dir = UPLOADS_DIR.to_path_buf();
    options
        .allowed_fields
        .push(MultipartFormDataField::raw("file"));
    options
        .allowed_fields
        .push(MultipartFormDataField::text("file_id"));
    options
        .allowed_fields
        .push(MultipartFormDataField::text("file_part_num"));

    let multipart_form_data = MultipartFormData::parse(content_type, data, options).unwrap();

    let file = multipart_form_data.raw.get("file");
    let file_id_text = multipart_form_data.texts.get("file_id");
    let file_part_num_text = multipart_form_data.texts.get("file_part_num");

    let mut file_id: Uuid = Uuid::nil();
    let mut file_part_num: u16 = 0;

    if let Some(file_id_text) = file_id_text {
        match file_id_text {
            TextField::Single(text) => file_id = Uuid::parse_str(&text.text).unwrap(),
            TextField::Multiple(_texts) => {
                // Because we only put one "text" field to the allowed_fields,
                // this arm will not be matched.
            }
        }
    }

    if let Some(file_part_num_text) = file_part_num_text {
        match file_part_num_text {
            TextField::Single(text) => {
                let _text = &text.text;
                file_part_num = *&text.text.parse::<u16>().unwrap();
            }
            TextField::Multiple(_texts) => {
                // Because we only put one "text" field to the allowed_fields,
                // this arm will not be matched.
            }
        }
    }

    let chunk = Chunk::new(file_id, *ID, file_part_num);
    let mut chunks_map = CHUNKS.write().unwrap();
    chunks_map.insert(chunk.id, chunk);

    if let Some(file) = file {
        match file {
            RawField::Single(file) => {
                use std::io::Write;
                let _content_type = &file.content_type;
                let content = &file.raw;

                let mut new_path = UPLOADS_DIR.to_path_buf();
                new_path.push(chunk.id.to_string());
                let mut f = File::create(new_path).unwrap();
                f.write(&content).unwrap();
            }
            RawField::Multiple(_files) => {
                // Because we only put one "file" field to the allowed_fields,
                // this arm will not be matched.
            }
        }
    }

    let metadata_server_url = env::var(METADATA_URL_KEY).unwrap();
    let _resp = reqwest::Client::new()
        .post(
            reqwest::Url::parse(format!("{}/api/chunk/completed", metadata_server_url).as_str())
                .unwrap(),
        )
        .json(&chunk)
        .send()
        .unwrap();
    json!(chunk)
}

#[get("/download/<chunk_id>")]
fn download(chunk_id: UuidRC) -> Option<Stream<File>> {
    let mut file_path = UPLOADS_DIR.to_path_buf();
    file_path.push(chunk_id.to_string());
    File::open(file_path).map(|file| Stream::from(file)).ok()
}

#[catch(404)]
fn not_found() -> JsonValue {
    json!({
        "status": "error",
        "reason": "Resource was not found."
    })
}

fn start_ping_job(address: String) {
    thread::spawn(move || loop {
        let metadata_server_url = env::var(METADATA_URL_KEY).unwrap();
        let _resp = reqwest::Client::new()
            .post(
                reqwest::Url::parse(format!("{}/api/ping", metadata_server_url).as_str()).unwrap(),
            )
            .header("x-chunk-server-id", ID.to_string())
            .header("x-chunk-server-address", &address)
            .send();
        thread::sleep(time::Duration::from_millis(5000))
    });
}

fn rocket() -> rocket::Rocket {
    rocket::ignite()
        .mount("/api", routes![multipart_upload, download])
        .register(catchers![not_found])
}

fn main() {
    CHUNKS.set(HashMap::new()).unwrap();

    match env::var(METADATA_URL_KEY) {
        Ok(_) => {
            let rocket_instance = rocket();
            let server_addr = format!(
                "http://{}:{}",
                rocket_instance.config().address,
                rocket_instance.config().port
            );
            start_ping_job(server_addr);

            rocket_instance.launch();
        }
        Err(_) => {
            println!("missing {} env variable", METADATA_URL_KEY);
        }
    }
}
