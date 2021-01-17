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
        for s in curr_dir_segments {
            nav = nav.child(s)?;
        }
        nav = nav.move_to(first)?;
    }

    for seg in segments {
        nav = nav.move_to(seg)?;
    }
    Ok(nav.get_path())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
