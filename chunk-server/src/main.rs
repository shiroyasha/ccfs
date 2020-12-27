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

use ccfs_commons::{init_value, Chunk};
use rocket::data::ToByteUnit;
use rocket::http::ContentType;
use rocket::response::Stream;
use rocket::Data;
use rocket_contrib::json::JsonValue;
use rocket_contrib::uuid::Uuid;
use rocket_multipart_form_data::{
    MultipartFormData, MultipartFormDataField, MultipartFormDataOptions,
};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::{thread, time};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

const METADATA_URL_KEY: &str = "METADATA_URL";
const SERVER_ADDRESS_KEY: &str = "SERVER_ADDRESS";

lazy_static! {
    // should be replaced with DB
    static ref UPLOADS_DIR: PathBuf = {
      let mut path_buf = dirs::home_dir().unwrap();
      path_buf.push("ccfs-uploads");
      path_buf
    };
    // static ref ID: Uuid = uuid_crate::Uuid::new_v4();
    static ref ID: Uuid = Uuid::from_str("cfc1b87a-2a58-4c17-a5ca-18232c535297").unwrap();
}

#[post("/upload", data = "<data>")]
async fn multipart_upload(content_type: &ContentType, data: Data) -> JsonValue {
    if !UPLOADS_DIR.exists() {
        fs::create_dir(UPLOADS_DIR.as_path()).await.unwrap();
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

    let limit = 64.mebibytes();
    let multipart_form_data = MultipartFormData::parse(content_type, data.open(limit), options)
        .await
        .unwrap();

    let file = multipart_form_data.raw.get("file");
    let file_id_text = multipart_form_data.texts.get("file_id");
    let file_part_num_text = multipart_form_data.texts.get("file_part_num");

    let mut file_id = init_value();
    let mut file_part_num: u16 = 0;

    if let Some(file_id_text) = file_id_text {
        let text = &file_id_text[0];
        file_id = Uuid::from_str(&text.text).unwrap();
    }

    if let Some(file_part_num_text) = file_part_num_text {
        let text = &file_part_num_text[0];
        let _text = &text.text;
        file_part_num = text.text.parse::<u16>().unwrap();
    }

    let chunk = Chunk::new(file_id, *ID, file_part_num);

    if let Some(raw_field) = file {
        let file = &raw_field[0];
        let _content_type = &file.content_type;
        let content = &file.raw;

        let mut new_path = UPLOADS_DIR.to_path_buf();
        new_path.push(chunk.id.to_string());
        let mut f = File::create(new_path).await.unwrap();
        f.write_all(&content).await.unwrap();
    }

    let metadata_server_url = env::var(METADATA_URL_KEY).unwrap();
    let _resp = reqwest::Client::new()
        .post(
            reqwest::Url::parse(format!("{}/api/chunk/completed", metadata_server_url).as_str())
                .unwrap(),
        )
        .json(&chunk)
        .send()
        .await
        .unwrap();
    json!(chunk)
}

#[get("/download/<chunk_id>")]
async fn download(chunk_id: Uuid) -> Option<Stream<File>> {
    let mut file_path = UPLOADS_DIR.to_path_buf();
    file_path.push(chunk_id.to_string());
    File::open(file_path).await.map(Stream::from).ok()
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

#[launch]
fn rocket() -> rocket::Rocket {
    match env::var(METADATA_URL_KEY) {
        Ok(_) => {
            let rocket_instance = rocket::ignite()
                .mount("/api", routes![multipart_upload, download])
                .register(catchers![not_found]);
            let server_addr = format!(
                "http://{}:{}",
                rocket_instance.config().address,
                rocket_instance.config().port
            );
            start_ping_job(server_addr);

            rocket_instance
        }
        Err(_) => {
            panic!("missing {} env variable", METADATA_URL_KEY);
        }
    }
}
