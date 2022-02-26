// use actix_http::http::StatusCode;
// use actix_web::{test, web, App};
// use ccfs_commons::{Chunk, FileMetadata, FileStatus};
// use metadata_server::routes::api::{get_chunks, signal_chuck_upload_completed};
// use metadata_server::{ChunksMap, FilesMap};
// use std::collections::{HashMap, HashSet};
// use std::sync::Arc;
// use test::{call_service, init_service, read_response_json, TestRequest};
// use tokio::sync::RwLock;
// use uuid::Uuid;

// #[actix_rt::test]
// async fn test_upload_completed_non_existing_file() -> std::io::Result<()> {
//     let chunk = Chunk::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
//     let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
//     let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
//     let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
//     let mut server = init_service(
//         App::new()
//             .data(chunks)
//             .data(files)
//             .data(metadata_tree)
//             .service(web::scope("/api").service(signal_chuck_upload_completed)),
//     )
//     .await;

//     let req = TestRequest::post()
//         .uri("/api/chunk/completed")
//         .set_json(&chunk)
//         .to_request();
//     let resp = call_service(&mut server, req).await;
//     assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
//     Ok(())
// }

// #[actix_rt::test]
// async fn test_upload_completed() -> std::io::Result<()> {
//     let mut map = HashMap::new();
//     let chunk = Chunk::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
//     let mut new_file = FileMetadata::create_file("test.txt".into(), 10, vec![chunk.id]);
//     let status = match &mut new_file.file_info {
//         ccfs_commons::FileInfo::File { id, status, .. } => {
//             *id = chunk.file_id;
//             status
//         }
//         _ => unreachable!(),
//     };
//     assert_eq!(status, &FileStatus::Started);
//     map.insert(chunk.file_id, (String::from(""), new_file.clone()));
//     let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
//     let files = Arc::new(RwLock::new(map));
//     let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
//     let mut server = init_service(
//         App::new()
//             .data(chunks.clone())
//             .data(files.clone())
//             .data(metadata_tree.clone())
//             .service(web::scope("/api").service(signal_chuck_upload_completed)),
//     )
//     .await;

//     let req = TestRequest::post()
//         .uri("/api/chunk/completed")
//         .set_json(&chunk)
//         .to_request();
//     let resp = call_service(&mut server, req).await;
//     assert_eq!(resp.status(), StatusCode::OK);

// let files_map = files.read().await;
// let tree = metadata_tree.read().await;
// let chunks_map = chunks.read().await;
// assert_eq!(chunks_map.len(), 1);
// let file = tree.traverse("test.txt").unwrap();
// assert_eq!(file.name, "test.txt");
// assert!(
//     matches!(file.file_info, ccfs_commons::FileInfo::File{status,..} if status == FileStatus::Completed)
// );
// assert_eq!(files_map.len(), 1);
// let (_path, f) = files_map.get(&chunk.file_id).unwrap();
// assert!(
//     matches!(f.file_info, ccfs_commons::FileInfo::File{status,..} if status == FileStatus::Completed)
// );

//     Ok(())
// }

// #[actix_rt::test]
// async fn test_upload_completed_part() -> std::io::Result<()> {
//     let mut map = HashMap::new();
//     let chunk = Chunk::new(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
//     let chunk2_id = Uuid::new_v4();
//     let mut new_file = FileMetadata::create_file("test.txt".into(), 10, vec![chunk.id, chunk2_id]);
//     let status = match &mut new_file.file_info {
//         ccfs_commons::FileInfo::File { id, status, .. } => {
//             *id = chunk.file_id;
//             status
//         }
//         _ => unreachable!(),
//     };
//     assert_eq!(status, &FileStatus::Started);
//     map.insert(chunk.file_id, (String::from(""), new_file.clone()));
//     let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
//     let files = Arc::new(RwLock::new(map));
//     let metadata_tree = Arc::new(RwLock::new(FileMetadata::create_root()));
//     let mut server = init_service(
//         App::new()
//             .data(chunks.clone())
//             .data(files.clone())
//             .data(metadata_tree.clone())
//             .service(web::scope("/api").service(signal_chuck_upload_completed)),
//     )
//     .await;

//     let req = TestRequest::post()
//         .uri("/api/chunk/completed")
//         .set_json(&chunk)
//         .to_request();
//     let resp = call_service(&mut server, req).await;
//     assert_eq!(resp.status(), StatusCode::OK);

// let files_map = files.read().await;
// let tree = metadata_tree.read().await;
// let chunks_map = chunks.read().await;
// assert_eq!(chunks_map.len(), 1);
// assert!(tree.traverse("test.txt").is_err());
// assert_eq!(files_map.len(), 1);
// let (_path, f) = files_map.get(&chunk.file_id).unwrap();
// assert!(
//     matches!(f.file_info, ccfs_commons::FileInfo::File{status,..} if status == FileStatus::Started)
// );

//     Ok(())
// }

// #[actix_rt::test]
// async fn test_get_file_chunks_not_existing_file() -> std::io::Result<()> {
//     let chunks: ChunksMap = Arc::new(RwLock::new(HashMap::new()));
//     let files: FilesMap = Arc::new(RwLock::new(HashMap::new()));
//     let mut server = init_service(
//         App::new()
//             .data(chunks)
//             .data(files)
//             .service(web::scope("/api").service(get_chunks)),
//     )
//     .await;

//     let unexisting_id = Uuid::new_v4();
//     let req = TestRequest::get()
//         .uri(&format!("/api/chunks/file/{}", unexisting_id))
//         .to_request();
//     let resp = call_service(&mut server, req).await;
//     assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
//     Ok(())
// }

// #[actix_rt::test]
// async fn test_get_file_chunks() -> std::io::Result<()> {
//     let mut chunks_map = HashMap::new();
//     let server1_id = Uuid::new_v4();
//     let server2_id = Uuid::new_v4();
//     let server3_id = Uuid::new_v4();
//     let file_id = Uuid::new_v4();
//     let chunk1 = Chunk::new(Uuid::new_v4(), file_id, server1_id);
//     let chunk2 = Chunk::new(Uuid::new_v4(), file_id, server1_id);
//     let chunk3 = Chunk::new(chunk1.id, file_id, server2_id);
//     let chunk4 = Chunk::new(chunk1.id, file_id, server3_id);
//     let chunk5 = Chunk::new(chunk2.id, file_id, server3_id);
//     let mut ch1_set = HashSet::new();
//     ch1_set.insert(chunk1);
//     ch1_set.insert(chunk3);
//     ch1_set.insert(chunk4);
//     let mut ch2_set = HashSet::new();
//     ch2_set.insert(chunk2);
//     ch2_set.insert(chunk5);
//     chunks_map.insert(chunk1.id, ch1_set.clone());
//     chunks_map.insert(chunk2.id, ch2_set.clone());
//     let mut file = FileMetadata::create_file("test1.txt".into(), 10, vec![chunk1.id, chunk2.id]);
//     match &mut file.file_info {
//         ccfs_commons::FileInfo::File { id, .. } => *id = file_id,
//         _ => unreachable!(),
//     };
//     let mut files_map = HashMap::new();
//     files_map.insert(file_id, (String::from(""), file.clone()));
//     let chunks = Arc::new(RwLock::new(chunks_map));
//     let files = Arc::new(RwLock::new(files_map));
//     let mut server = init_service(
//         App::new()
//             .data(chunks)
//             .data(files)
//             .service(web::scope("/api").service(get_chunks)),
//     )
//     .await;

//     let req = TestRequest::get()
//         .uri(&format!("/api/chunks/file/{}", file_id))
//         .to_request();
//     let data: Vec<Vec<Chunk>> = read_response_json(&mut server, req).await;
//     assert_eq!(data.len(), 2);
//     assert_eq!(data[0].len(), 3);
//     assert!(ch1_set.iter().all(|val| data[0].contains(val)));
//     assert_eq!(data[1].len(), 2);
//     assert!(ch2_set.iter().all(|val| data[1].contains(val)));
//     Ok(())
// }
