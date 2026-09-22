#[cfg(not(feature = "std"))]
use alloc::string::String;

/// The scheme of an ofdb URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriScheme {
    /// In-memory database (`:in_memory:`).
    InMemory,
    /// File-backed database (`ofdb://<path>`).
    File,
}

/// A parsed ofdb URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uri {
    pub scheme: UriScheme,
    /// The path component. For `InMemory` this is always `None`.
    /// For `File` this is always `Some`.
    pub path: Option<String>,
}

/// Errors from URI parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriError {
    /// The URI did not match any known scheme.
    UnsupportedScheme,
    /// A file URI had no path.
    MissingPath,
}

/// Parse an ofdb URI string.
///
/// Supported formats:
/// - `:in_memory:` → `UriScheme::InMemory`
/// - `ofdb://<path>` → `UriScheme::File` with the path after `ofdb://`
///
/// Returns `UriError::UnsupportedScheme` for unknown schemes.
/// Returns `UriError::MissingPath` for `ofdb://` with no path.
pub fn parse_uri(uri: &str) -> Result<Uri, UriError> {
    if let Some(rest) = uri.strip_prefix(":in_memory:") {
        // The sentinel ends with `:`; the full match means rest is empty.
        if rest.is_empty() {
            return Ok(Uri {
                scheme: UriScheme::InMemory,
                path: None,
            });
        }
    }

    if let Some(rest) = uri.strip_prefix("ofdb://") {
        if rest.is_empty() {
            return Err(UriError::MissingPath);
        }
        return Ok(Uri {
            scheme: UriScheme::File,
            path: Some(rest.to_string()),
        });
    }

    Err(UriError::UnsupportedScheme)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_in_memory() {
        assert_eq!(
            parse_uri(":in_memory:").unwrap(),
            Uri {
                scheme: UriScheme::InMemory,
                path: None,
            }
        );
    }

    #[test]
    fn parse_file_relative() {
        let uri = parse_uri("ofdb://./local.db").unwrap();
        assert_eq!(uri.scheme, UriScheme::File);
        assert_eq!(uri.path.unwrap(), "./local.db");
    }

    #[test]
    fn parse_file_relative_nested() {
        let uri = parse_uri("ofdb://tmp/data.db").unwrap();
        assert_eq!(uri.scheme, UriScheme::File);
        assert_eq!(uri.path.unwrap(), "tmp/data.db");
    }

    #[test]
    fn parse_file_absolute() {
        let uri = parse_uri("ofdb:///abs/path.db").unwrap();
        assert_eq!(uri.scheme, UriScheme::File);
        assert_eq!(uri.path.unwrap(), "/abs/path.db");
    }

    #[test]
    fn parse_file_empty_path() {
        assert_eq!(parse_uri("ofdb://"), Err(UriError::MissingPath));
    }

    #[test]
    fn parse_unknown_scheme() {
        assert_eq!(
            parse_uri("sqlite:///foo.db"),
            Err(UriError::UnsupportedScheme)
        );
    }
}
