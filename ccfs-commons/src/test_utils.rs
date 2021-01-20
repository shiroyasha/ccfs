use crate::result::CCFSResult;
use crate::FileMetadata;
use std::str::FromStr;
use uuid::Uuid;

pub fn add_dir2(trie: &mut FileMetadata) -> CCFSResult<()> {
    trie.insert_dir("dir2")?;
    let dir2 = trie.traverse_mut("dir2")?;
    dir2.insert_file(
        "test.txt",
        10,
        vec![Uuid::from_str("1a6e7006-12a7-4935-b8c0-58fa7ea84b09").unwrap()],
    )?;
    dir2.insert_dir("subdir")?;
    let subdir = dir2.traverse_mut("subdir")?;
    subdir.insert_dir("tmp")?;
    subdir.insert_file(
        "file",
        100,
        vec![Uuid::from_str("6d53a85f-505b-4a1a-ae6d-f7c18761d04a").unwrap()],
    )?;
    Ok(())
}

pub fn build_tree() -> CCFSResult<FileMetadata> {
    let mut trie = FileMetadata::create_root();
    trie.insert_dir("dir1")?;
    add_dir2(&mut trie)?;
    trie.insert_file(
        "some.zip",
        0,
        vec![Uuid::from_str("ec73d743-050b-4f52-992a-d1102340d739").unwrap()],
    )?;

    Ok(trie)
}
