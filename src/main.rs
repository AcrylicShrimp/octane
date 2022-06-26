mod file_allocator;

use actix_multipart::Multipart;
use actix_web::{
    get, post, put, web, App, Error, HttpResponse, HttpServer, Responder, ResponseError,
};
use blake3::{Hash, Hasher};
use bytes::BytesMut;
use derive_more::{Display, Error};
use file_allocator::FileAllocator;
use futures_util::{Stream, TryStreamExt};
use meilisearch_sdk::{client::Client, indexes::Index};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct IndexedFile {
    uuid: String,
    name: String,
    size: u64,
    mime: Option<String>,
    hash: String,
    indexed_at: u128,
    tags: Vec<IndexedTag>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexedTag {
    name: String,
    value: String,
}

struct Context {
    name: String,
}

#[post("/files")]
async fn provision_file(file_allocator: web::Data<FileAllocator>) -> impl Responder {
    HttpResponse::Created().body(file_allocator.allocate().to_string())
}

#[derive(Debug, Display, Error)]
#[display(fmt = "invalid uuid")]
struct UuidError;

impl ResponseError for UuidError {}

#[put("/files/{uuid}")]
async fn upload_file(
    file_allocator: web::Data<FileAllocator>,
    ms_index: web::Data<Index>,
    uuid: web::Path<String>,
    mut payload: Multipart,
) -> Result<impl Responder, Error> {
    let uuid = uuid.into_inner();
    let uuid = Uuid::parse_str(&uuid).map_err(|_| UuidError)?;
    println!("uuid: {}", uuid);

    if !file_allocator.mark_as_uploading(uuid) {
        return Err(UuidError.into());
    }

    let mut is_file_seen = false;
    let mut tags = Vec::new();
    let mut indexed_file = None;

    while let Some(mut field) = payload.try_next().await? {
        if let Some(filename) = field
            .content_disposition()
            .get_filename()
            .map(|s| s.to_owned())
        {
            if is_file_seen {
                continue;
            }
            is_file_seen = true;

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(Path::new("files").join(uuid.to_string()))
                .await?;
            while let Some(chunk) = field.try_next().await? {
                // TODO: Should we cancel the upload if the file is too large?
                file.write_all(&chunk).await?;
            }

            println!("file as been written");

            let file_size = file.metadata().await?.len();
            println!("file size: {}", file_size);

            let file = OpenOptions::new()
                .read(true)
                .open(Path::new("files").join(uuid.to_string()))
                .await?;
            let mut file = file.into_std().await;
            let mut hasher = Hasher::new();
            let hash = web::block(move || -> Result<Hash, std::io::Error> {
                std::io::copy(&mut file, &mut hasher)?;
                Ok(hasher.finalize())
            })
            .await??;
            println!("file hash: {}", hash);

            let filename = sanitize_filename::sanitize(filename);
            indexed_file = Some(IndexedFile {
                uuid: uuid.to_string(),
                size: file_size,
                mime: if let Some(mime) = mime_guess::from_path(&filename).first() {
                    Some(mime.to_string())
                } else {
                    None
                },
                hash: hash.to_hex().to_string(),
                name: filename,
                // TODO: Remove the unwrap here.
                indexed_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                tags: vec![],
            });
        } else {
            let mut value = match field.size_hint() {
                (_, Some(to)) => BytesMut::with_capacity(to),
                (from, _) => BytesMut::with_capacity(from),
            };

            while let Some(chunk) = field.try_next().await? {
                // TODO: Should we cancel the upload if the tag value is too large?
                value.extend_from_slice(&chunk);
            }

            tags.push(IndexedTag {
                name: field.name().to_string(),
                value: String::from_utf8_lossy(&value).to_string(),
            });
        }
    }

    let mut indexed_file = if let Some(indexed_file) = indexed_file {
        indexed_file
    } else {
        return Ok(HttpResponse::BadRequest().body("no file uploaded"));
    };

    indexed_file.tags = tags;
    println!("indexed file: {:?}", &indexed_file);

    ms_index
        .add_documents(&[indexed_file], Some("uuid"))
        .await
        .unwrap();
    file_allocator.free(&uuid);

    Ok(HttpResponse::Ok().finish())
}

#[get("/")]
async fn index(context: web::Data<Context>) -> impl Responder {
    HttpResponse::Ok().body(format!("Hello world, {}!", context.name))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let client = Client::new("http://localhost:7700", "supertester");
    client.create_index("files", Some("uuid")).await.unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Context {
                name: "test".to_owned(),
            }))
            .app_data(web::Data::new(FileAllocator::new()))
            .app_data(web::Data::new(client.index("files")))
            .service(index)
            .service(provision_file)
            .service(upload_file)
    })
    .bind(("127.0.0.1", 80))?
    .run()
    .await
}
