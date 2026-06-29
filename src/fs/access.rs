// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FsScheme {
    Local,
    ObjectStore,
    Hdfs,
    Memory,
}

impl FsScheme {
    pub fn is_object_store(self) -> bool {
        self == Self::ObjectStore
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsLocation {
    original: String,
    scheme: FsScheme,
    uri_scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl FsLocation {
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, String> {
        let original = raw.as_ref().trim();
        if original.is_empty() {
            return Err("fs location is empty".to_string());
        }

        let Some((uri_scheme, rest)) = split_uri_scheme(original) else {
            return Ok(Self::local(original, None, original));
        };
        let uri_scheme = uri_scheme.to_ascii_lowercase();

        match uri_scheme.as_str() {
            "file" => Self::parse_file(original, uri_scheme, rest),
            "s3" | "s3a" | "oss" => {
                let (authority, path) =
                    parse_authority_and_path(original, rest, true, uri_scheme.as_str())?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::ObjectStore,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            "hdfs" => {
                let (authority, path) = parse_authority_and_path(original, rest, true, "hdfs")?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::Hdfs,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            "memory" => {
                let (authority, path) = parse_authority_and_path(original, rest, false, "memory")?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::Memory,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            _ => Err(format!("unsupported fs location scheme: {original}")),
        }
    }

    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn uri_scheme(&self) -> Option<&str> {
        self.uri_scheme.as_deref()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn local(original: &str, uri_scheme: Option<String>, path: &str) -> Self {
        Self {
            original: original.to_string(),
            scheme: FsScheme::Local,
            uri_scheme,
            authority: None,
            path: path.to_string(),
        }
    }

    fn parse_file(original: &str, uri_scheme: String, rest: &str) -> Result<Self, String> {
        if let Some(without_prefix) = rest.strip_prefix("//") {
            if without_prefix.starts_with('/') {
                ensure_non_empty_path(original, "file", without_prefix)?;
                return Ok(Self::local(original, Some(uri_scheme), without_prefix));
            }

            let (authority, path) = without_prefix
                .split_once('/')
                .unwrap_or((without_prefix, ""));
            if !authority.is_empty() && authority != "localhost" {
                return Err(format!("unsupported fs file authority: {original}"));
            }
            let path = if path.is_empty() {
                ""
            } else {
                &without_prefix[authority.len()..]
            };
            ensure_non_empty_path(original, "file", path)?;
            return Ok(Self::local(original, Some(uri_scheme), path));
        }

        ensure_non_empty_path(original, "file", rest)?;
        Ok(Self::local(original, Some(uri_scheme), rest))
    }
}

fn split_uri_scheme(raw: &str) -> Option<(&str, &str)> {
    let colon = raw.find(':')?;
    let scheme = &raw[..colon];
    if scheme.is_empty() || !scheme.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    if !scheme
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-'))
    {
        return None;
    }
    Some((scheme, &raw[colon + 1..]))
}

fn parse_authority_and_path(
    original: &str,
    rest: &str,
    authority_required: bool,
    scheme_label: &str,
) -> Result<(Option<String>, String), String> {
    let Some(without_prefix) = rest.strip_prefix("//") else {
        return Err(format!("unsupported fs location scheme: {original}"));
    };

    let (authority, path) = if without_prefix.starts_with('/') {
        (None, without_prefix.trim_start_matches('/').to_string())
    } else {
        let (authority, path) = without_prefix
            .split_once('/')
            .unwrap_or((without_prefix, ""));
        let authority = if authority.is_empty() {
            None
        } else {
            Some(authority.to_string())
        };
        (authority, path.trim_start_matches('/').to_string())
    };

    if authority_required && authority.is_none() {
        return Err(format!("fs location authority is empty: {original}"));
    }
    ensure_non_empty_path(original, scheme_label, &path)?;

    Ok((authority, path))
}

fn ensure_non_empty_path(original: &str, scheme_label: &str, path: &str) -> Result<(), String> {
    if path.trim_start_matches('/').is_empty() {
        return Err(format!("{scheme_label} location missing path: {original}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_local_path() {
        let loc = FsLocation::parse("/tmp/data/a.parquet").expect("parse local path");
        assert_eq!(loc.scheme(), FsScheme::Local);
        assert_eq!(loc.uri_scheme(), None);
        assert_eq!(loc.authority(), None);
        assert_eq!(loc.path(), "/tmp/data/a.parquet");
        assert_eq!(loc.original(), "/tmp/data/a.parquet");
    }

    #[test]
    fn trims_raw_input_before_parsing() {
        let loc = FsLocation::parse("  /tmp/data/a.parquet  ").expect("parse trimmed path");
        assert_eq!(loc.path(), "/tmp/data/a.parquet");
        assert_eq!(loc.original(), "/tmp/data/a.parquet");
    }

    #[test]
    fn parses_file_uri_variants_as_local() {
        let loc = FsLocation::parse("file:///tmp/data/a.parquet").expect("parse file URI");
        assert_eq!(loc.scheme(), FsScheme::Local);
        assert_eq!(loc.uri_scheme(), Some("file"));
        assert_eq!(loc.path(), "/tmp/data/a.parquet");

        let localhost =
            FsLocation::parse("file://localhost/tmp/data/a.parquet").expect("parse localhost URI");
        assert_eq!(localhost.scheme(), FsScheme::Local);
        assert_eq!(localhost.uri_scheme(), Some("file"));
        assert_eq!(localhost.path(), "/tmp/data/a.parquet");
    }

    #[test]
    fn rejects_file_locations_without_path() {
        for raw in ["file://localhost", "file:/"] {
            let err = FsLocation::parse(raw).expect_err("file path is required");
            assert!(err.contains("file location missing path"), "{err}");
        }
    }

    #[test]
    fn parses_object_store_locations() {
        for raw in [
            "s3://bucket/warehouse/t/a.parquet",
            "s3a://bucket/warehouse/t/a.parquet",
            "oss://bucket/warehouse/t/a.parquet",
        ] {
            let loc = FsLocation::parse(raw).expect("parse object-store location");
            assert_eq!(loc.scheme(), FsScheme::ObjectStore);
            assert_eq!(loc.authority(), Some("bucket"));
            assert_eq!(loc.path(), "warehouse/t/a.parquet");
        }
    }

    #[test]
    fn rejects_object_store_locations_without_path() {
        let err = FsLocation::parse("s3://bucket").expect_err("s3 path is required");
        assert!(err.contains("s3 location missing path"), "{err}");
    }

    #[test]
    fn parses_hdfs_location() {
        let loc = FsLocation::parse("hdfs://nn-1:9000/user/hive/a.parquet").expect("parse hdfs");
        assert_eq!(loc.scheme(), FsScheme::Hdfs);
        assert_eq!(loc.uri_scheme(), Some("hdfs"));
        assert_eq!(loc.authority(), Some("nn-1:9000"));
        assert_eq!(loc.path(), "user/hive/a.parquet");
    }

    #[test]
    fn rejects_hdfs_location_without_path() {
        let err = FsLocation::parse("hdfs://nn-1:9000").expect_err("hdfs path is required");
        assert!(err.contains("hdfs location missing path"), "{err}");
    }

    #[test]
    fn parses_memory_locations() {
        let loc = FsLocation::parse("memory://warehouse/table/data.parquet")
            .expect("parse memory location");
        assert_eq!(loc.scheme(), FsScheme::Memory);
        assert_eq!(loc.uri_scheme(), Some("memory"));
        assert_eq!(loc.authority(), Some("warehouse"));
        assert_eq!(loc.path(), "table/data.parquet");

        let metadata = FsLocation::parse("memory:///metadata/test.avro")
            .expect("parse memory URI without authority");
        assert_eq!(metadata.scheme(), FsScheme::Memory);
        assert_eq!(metadata.authority(), None);
        assert_eq!(metadata.path(), "metadata/test.avro");
    }

    #[test]
    fn rejects_memory_location_without_path() {
        let err = FsLocation::parse("memory://warehouse").expect_err("memory path is required");
        assert!(err.contains("memory location missing path"), "{err}");
    }

    #[test]
    fn rejects_unsupported_scheme() {
        let err = FsLocation::parse("ftp://host/path").expect_err("ftp is unsupported");
        assert!(err.contains("unsupported fs location scheme"), "{err}");
    }
}
