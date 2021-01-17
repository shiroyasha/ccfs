use crate::FileMetadata;
use crate::{errors::Error::*, result::CCFSResult};
use regex::Regex;
use std::str::SplitTerminator;

/// Path validator for CCFS
/// it allows unix-like paths
pub fn parse_path(path: &str) -> CCFSResult<SplitTerminator<char>> {
    let segment_re =
        Regex::new(r"^\.{1,2}$|[A-Za-z0-9-_+.~*()'\[\]\{\}&%$#@!|]*[A-Za-z0-9][A-Za-z0-9-_+.~*()'\[\]\{\}&%$#@!|]*").unwrap();
    if path.is_empty() {
        return Err(InvalidPath {
            msg: "Path cannot be empty".into(),
        }
        .into());
    }
    let mut segments = path.split_terminator('/').enumerate().peekable();
    let (_, first) = segments.next().expect("path is empty");
    if !first.is_empty() && !segment_re.is_match(first) {
        return Err(InvalidPath {
            msg: format!("{} is not valid", first),
        }
        .into());
    }
    while let Some((_pos, next)) = segments.next() {
        if next.is_empty() && segments.peek().is_some() {
            return Err(InvalidPath {
                msg: "Cannot have empty path segment -> //".into(),
            }
            .into());
        } else if !segment_re.is_match(next) {
            return Err(InvalidPath {
                msg: format!("{} is not valid", next),
            }
            .into());
        }
    }
    Ok(path.split_terminator('/'))
}

pub fn evaluate_path(curr_dir: &str, tree: &FileMetadata, path: &str) -> CCFSResult<String> {
    let mut segments = parse_path(&path)?.peekable();
    let mut nav = tree.navigate();
    let first = segments.next().unwrap();
    if !first.is_empty() {
        let curr_dir_segments = curr_dir.split_terminator('/');
        for s in curr_dir_segments.skip(1) {
            nav = nav.child(s)?;
        }
        nav = nav.move_to(first)?;
    }

    while let Some(seg) = segments.next() {
        nav = nav.move_to(seg)?;
        if segments.peek().is_some() || path.ends_with('/') {
            // can navigate through directories,
            // only the last segment can be a file
            nav.node.children()?;
        }
    }
    Ok(nav.get_path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::file::tests::build;
    use crate::ROOT_DIR;

    #[test]
    fn test_validate_path() -> CCFSResult<()> {
        assert_eq!(parse_path(".")?.collect::<Vec<_>>(), ["."]);
        assert_eq!(parse_path("..")?.collect::<Vec<_>>(), [".."]);
        assert_eq!(parse_path("/")?.collect::<Vec<_>>(), [""]);
        assert_eq!(
            parse_path("./some dir")?.collect::<Vec<_>>(),
            [".", "some dir"]
        );
        assert_eq!(
            parse_path("dir/some dir")?.collect::<Vec<_>>(),
            ["dir", "some dir"]
        );
        assert_eq!(parse_path("dir/.")?.collect::<Vec<_>>(), ["dir", "."]);
        assert_eq!(
            parse_path("/../repo")?.collect::<Vec<_>>(),
            ["", "..", "repo"]
        );
        assert_eq!(
            parse_path("/../.....d")?.collect::<Vec<_>>(),
            ["", "..", ".....d"]
        );
        assert_eq!(
            parse_path("/../file.txt/")?.collect::<Vec<_>>(),
            ["", "..", "file.txt"]
        );
        assert_eq!(
            parse_path("/../file.txt")?.collect::<Vec<_>>(),
            ["", "..", "file.txt"]
        );
        assert_eq!(
            parse_path("/../.....").unwrap_err().to_string(),
            "Invalid path: ..... is not valid"
        );
        assert_eq!(
            parse_path("//test.txt").unwrap_err().to_string(),
            "Invalid path: Cannot have empty path segment -> //"
        );
        assert_eq!(
            parse_path("/dir/*").unwrap_err().to_string(),
            "Invalid path: * is not valid"
        );
        Ok(())
    }

    #[test]
    fn test_evaluate_path() -> CCFSResult<()> {
        let tree = build()?;
        assert_eq!(evaluate_path("/", &tree, "dir1")?, "/dir1");
        assert_eq!(
            evaluate_path("/", &tree, "dir1/file.txt")
                .unwrap_err()
                .to_string(),
            "Path 'file.txt' doesn't exist"
        );
        assert_eq!(evaluate_path("/dir1", &tree, ".")?, "/dir1");
        assert_eq!(evaluate_path("/dir1", &tree, "..")?, ROOT_DIR);
        assert_eq!(evaluate_path("/dir1", &tree, "../../../..")?, ROOT_DIR);
        assert_eq!(
            evaluate_path("/dir2", &tree, "./subdir/tmp/..")?,
            "/dir2/subdir"
        );
        assert_eq!(
            evaluate_path("/dir2", &tree, "./subdir/file/..")
                .unwrap_err()
                .to_string(),
            "'file' is not a directory"
        );
        assert_eq!(
            evaluate_path("/dir2/subdir/tmp", &tree, "../././tmp/../")?,
            "/dir2/subdir"
        );
        assert_eq!(
            evaluate_path("/dir2/subdir/tmp", &tree, "../././tmp/../file")?,
            "/dir2/subdir/file"
        );
        assert_eq!(
            evaluate_path("/dir2/subdir/tmp", &tree, "../././tmp/../file/")
                .unwrap_err()
                .to_string(),
            "'file' is not a directory"
        );
        Ok(())
    }
}
