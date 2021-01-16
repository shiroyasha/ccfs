use regex::Regex;

/// Path validator for CCFS
/// it allows unix-like paths
pub fn is_valid_path(path: &str) -> bool {
    let segment_re =
        Regex::new(r"^\.{1,2}$|[A-Za-z0-9-_+.~*()'\[\]\{\}&%$#@!|]*[A-Za-z0-9][A-Za-z0-9-_+.~*()'\[\]\{\}&%$#@!|]*").unwrap();
    if path.is_empty() {
        return false;
    }
    let mut segments = path.split_terminator('/').peekable();
    let first = segments.next().unwrap();
    if !first.is_empty() && !segment_re.is_match(first) {
        return false;
    }
    while segments.peek().is_some() {
        let next = segments.next().unwrap();
        if next.is_empty() && segments.peek().is_some() || !segment_re.is_match(next) {
            return false;
        }
    }
    true
}

pub fn evaluate_path(path: String) {
    if !is_valid_path(&path) {
        //
    }
    //
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path() {
        assert!(is_valid_path("."));
        assert!(is_valid_path(".."));
        assert!(is_valid_path("/"));
        assert!(is_valid_path("./some dir"));
        assert!(is_valid_path("dir/some dir"));
        assert!(is_valid_path("dir/."));
        assert!(is_valid_path("/../repo"));
        assert!(is_valid_path("/../.....d"));
        assert!(is_valid_path("/../file.txt/"));
        assert!(is_valid_path("/../file.txt"));
        assert!(!is_valid_path("/../....."));
        assert!(!is_valid_path("//test.txt"));
        assert!(!is_valid_path("/dir/*"));
    }
}
