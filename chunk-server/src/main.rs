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
use rocket::data::ToByteUnit;
use rocket::http::{ContentType, Status};
use rocket::request::Request;
use rocket::response::{Responder, Stream};
use rocket::{Data, Response, State};
use rocket_contrib::json::JsonValue;
use rocket_contrib::uuid::Uuid;
use rocket_multipart_form_data::{
    MultipartFormData, MultipartFormDataField, MultipartFormDataOptions,
};
use snafu::{ResultExt, Snafu};
use std::env;
use std::path::PathBuf;
use std::str::FromStr;
use std::{thread, time};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

const METADATA_URL_KEY: &str = "METADATA_URL";
const SERVER_ID_KEY: &str = "SERVER_ID";

lazy_static! {
    static ref UPLOADS_DIR: PathBuf = {
        let mut path_buf = dirs::home_dir().expect("Couldn't determine home dir");
        path_buf.push("ccfs-uploads");
        path_buf
    };
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to create {}: {}", path.display(), source))]
    IOCreate {
        source: tokio::io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to write to {}: {}", path.display(), source))]
    IOWrite {
        source: tokio::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to parse multipart form data: {}", source))]
    ParseData {
        source: rocket_multipart_form_data::MultipartFormDataError,
    },

    #[snafu(display("Unable to parse uuid {}: {}", text, source))]
    ParseUuid {
        source: rocket_contrib::uuid::uuid_crate::Error,
        text: String,
    },
    #[snafu(display("Unable to parse number {}: {}", text, source))]
    ParseNumber {
        source: std::num::ParseIntError,
        text: String,
    },
    #[snafu(display("Communication error with metadata server: {}", source))]
    MetaServerCommunication { source: reqwest::Error },
    #[snafu(display("Missing form part {}", name))]
    MissingPart { name: String },
}

impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _request: &'r Request<'_>) -> rocket::response::Result<'static> {
        Response::build().status(Status::InternalServerError).ok()
    }
}

type Result<T, E = Error> = std::result::Result<T, E>;

struct MetadataUrl(&'static str);
struct ServerID(&'static Uuid);

#[post("/upload", data = "<data>")]
async fn multipart_upload(
    metadata_url: State<'_, MetadataUrl>,
    server_id: State<'_, ServerID>,
    content_type: &ContentType,
    data: Data,
) -> Result<JsonValue> {
    if !UPLOADS_DIR.exists() {
        fs::create_dir(UPLOADS_DIR.as_path())
            .await
            .context(IOCreate {
                path: UPLOADS_DIR.as_path(),
            })?;
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
        .context(ParseData)?;

    let file_id_text = multipart_form_data
        .texts
        .get("file_id")
        .ok_or_else(|| MissingPart { name: "file_id" }.build())?[0]
        .text
        .clone();
    let file_id = Uuid::from_str(&file_id_text).context(ParseUuid {
        text: file_id_text.clone(),
    })?;
    let file_part_num_text = multipart_form_data
        .texts
        .get("file_part_num")
        .ok_or_else(|| {
            MissingPart {
                name: "file_part_num",
            }
            .build()
        })?[0]
        .text
        .clone();
    let file_part_num = file_part_num_text.parse::<u16>().context(ParseNumber {
        text: file_part_num_text.clone(),
    })?;
    let file = &multipart_form_data
        .raw
        .get("file")
        .ok_or_else(|| MissingPart { name: "file" }.build())?[0];

    let chunk = Chunk::new(file_id, *server_id.0, file_part_num);

    let content = &file.raw;

    let mut new_path = UPLOADS_DIR.to_path_buf();
    new_path.push(chunk.id.to_string());
    let mut f = File::create(&new_path)
        .await
        .context(IOCreate { path: &new_path })?;
    f.write_all(&content)
        .await
        .context(IOWrite { path: new_path })?;

    let _resp = reqwest::Client::new()
        .post(&format!("{}/api/chunk/completed", metadata_url.0))
        .json(&chunk)
        .send()
        .await
        .context(MetaServerCommunication)?;
    Ok(json!(chunk))
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

fn start_ping_job(address: String, metadata_url: String, server_id: String) {
    thread::spawn(move || -> Result<()> {
        loop {
            let _resp = reqwest::Client::new()
                .post(&format!("{}/api/ping", &metadata_url))
                .header("x-chunk-server-id", &server_id)
                .header("x-chunk-server-address", &address)
                .send();
            thread::sleep(time::Duration::from_millis(5000))
        }
    });
}

#[launch]
fn rocket() -> rocket::Rocket {
    let metadata_url = env::var(METADATA_URL_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", METADATA_URL_KEY));
    let server_id = env::var(SERVER_ID_KEY)
        .unwrap_or_else(|_| panic!("missing {} env variable", SERVER_ID_KEY));

    let rocket_instance = rocket::ignite()
        .mount("/api", routes![multipart_upload, download])
        .register(catchers![not_found])
        .manage(MetadataUrl(Box::leak(
            metadata_url.clone().into_boxed_str(),
        )))
        .manage(ServerID(Box::leak(Box::new(
            Uuid::from_str(&server_id).expect("Server ID is not valid"),
        ))));
    let server_addr = format!(
        "http://{}:{}",
        rocket_instance.config().address,
        rocket_instance.config().port
    );
    start_ping_job(server_addr, metadata_url, server_id);

    rocket_instance
}
