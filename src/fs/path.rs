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
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ScanPathScheme {
    Local,
    Oss,
    Hdfs,
}

fn normalize_local_scan_path(raw: &str) -> Result<String, String> {
    let path = raw.trim();
    if let Some(rest) = path.strip_prefix("file://") {
        if rest.is_empty() {
            return Err("invalid file URI: empty path".to_string());
        }
        if let Some(abs) = rest.strip_prefix('/') {
            return Ok(format!("/{}", abs));
        }
        if let Some(host_path) = rest.strip_prefix("localhost/") {
            return Ok(format!("/{}", host_path));
        }
        return Err(format!("unsupported file URI host in local path: {path}"));
    }
    if let Some(rest) = path.strip_prefix("file:/") {
        return Ok(format!("/{}", rest.trim_start_matches('/')));
    }
    Ok(path.to_string())
}

pub fn classify_scan_paths<'a, I>(paths: I) -> Result<ScanPathScheme, String>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut scheme: Option<ScanPathScheme> = None;
    for raw in paths {
        let path = raw.trim();
        if path.is_empty() {
            return Err("scan path is empty".to_string());
        }
        let current = if path.starts_with("oss://")
            || path.starts_with("s3://")
            || path.starts_with("s3a://")
        {
            ScanPathScheme::Oss
        } else if path.starts_with("hdfs://") {
            ScanPathScheme::Hdfs
        } else if path.starts_with("file:/")
            || path.starts_with("file://")
            || path.starts_with('/')
            || !path.contains("://")
        {
            ScanPathScheme::Local
        } else {
            return Err(format!("unsupported scan path scheme: {path}"));
        };
        if let Some(prev) = scheme {
            if prev != current {
                return Err("mixed scan path schemes are not allowed".to_string());
            }
        } else {
            scheme = Some(current);
        }
    }
    scheme.ok_or_else(|| "scan paths are empty".to_string())
}

pub struct ResolvedScanPaths {
    pub scheme: ScanPathScheme,
    pub root: Option<String>,
    pub paths: Vec<String>,
}

pub fn resolve_opendal_paths(
    paths: &[String],
    object_store_cfg: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<(opendal::Operator, ResolvedScanPaths), String> {
    let handle = crate::fs::access::FsAccessResolver::new()
        .resolve_locations(paths.iter().map(|s| s.as_str()), object_store_cfg)?;
    let scheme = match handle.scheme() {
        crate::fs::access::FsScheme::Local => ScanPathScheme::Local,
        crate::fs::access::FsScheme::ObjectStore => ScanPathScheme::Oss,
        crate::fs::access::FsScheme::Hdfs => ScanPathScheme::Hdfs,
    };
    let resolved = ResolvedScanPaths {
        scheme,
        root: handle.root().map(str::to_string),
        paths: handle
            .paths()
            .iter()
            .map(|path| path.operator_relative_path().to_string())
            .collect(),
    };
    Ok((handle.operator(), resolved))
}

pub fn resolve_object_store_operator_and_path(
    full_path: &str,
    cfg: &crate::fs::object_store::ObjectStoreConfig,
) -> Result<(opendal::Operator, String), String> {
    let handle =
        crate::fs::access::FsAccessResolver::new().resolve_location(full_path, Some(cfg))?;
    if handle.scheme() != crate::fs::access::FsScheme::ObjectStore {
        return Err(format!("expected object-store path, got {full_path}"));
    }
    let rel = handle
        .paths()
        .first()
        .ok_or_else(|| format!("resolved empty path list for {full_path}"))?
        .operator_relative_path()
        .to_string();
    Ok((handle.operator(), rel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_local_scan_path_keeps_plain_absolute_path() {
        let path = "/tmp/a.parquet";
        let got = normalize_local_scan_path(path).expect("normalize plain absolute path");
        assert_eq!(got, path);
    }

    #[test]
    fn normalize_local_scan_path_supports_file_uri_variants() {
        let p1 = normalize_local_scan_path("file:/tmp/a.parquet").expect("file:/ path");
        let p2 = normalize_local_scan_path("file:///tmp/a.parquet").expect("file:/// path");
        let p3 =
            normalize_local_scan_path("file://localhost/tmp/a.parquet").expect("localhost path");
        assert_eq!(p1, "/tmp/a.parquet");
        assert_eq!(p2, "/tmp/a.parquet");
        assert_eq!(p3, "/tmp/a.parquet");
    }

    #[test]
    fn normalize_local_scan_path_rejects_non_local_file_uri_host() {
        let err = normalize_local_scan_path("file://remote-host/tmp/a.parquet")
            .expect_err("non-local host should be rejected");
        assert!(err.contains("unsupported file URI host"));
    }

    #[test]
    fn classify_scan_paths_accepts_file_uri_as_local() {
        let scheme = classify_scan_paths(["file:/tmp/a.parquet"]).expect("classify file URI path");
        assert_eq!(scheme, ScanPathScheme::Local);
    }

    #[test]
    fn classify_scan_paths_accepts_hdfs_uri() {
        let scheme = classify_scan_paths(["hdfs://nn-1:9000/user/hive/a.parquet"])
            .expect("classify hdfs URI path");
        assert_eq!(scheme, ScanPathScheme::Hdfs);
    }

    #[test]
    fn classify_scan_paths_accepts_s3a_uri_as_object_store() {
        let scheme = classify_scan_paths(["s3a://bucket/warehouse/t/data.parquet"])
            .expect("classify s3a URI path");
        assert_eq!(scheme, ScanPathScheme::Oss);
    }

    #[test]
    fn resolve_opendal_paths_uses_credentials_only_object_store_config() {
        let cfg = crate::fs::object_store::ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };

        let paths = vec!["s3://bucket-a/warehouse/t/data.parquet".to_string()];
        let (_op, resolved) = resolve_opendal_paths(&paths, Some(&cfg)).expect("resolve paths");

        assert_eq!(resolved.scheme, ScanPathScheme::Oss);
        assert_eq!(resolved.paths, vec!["warehouse/t/data.parquet"]);
        assert_eq!(resolved.root, None);
    }

    #[test]
    fn classify_scan_paths_rejects_memory_uri() {
        let err = classify_scan_paths(["memory://warehouse/table/data.parquet"])
            .expect_err("memory is not a NovaRocks scan path");
        assert!(err.contains("unsupported scan path scheme"), "{err}");
    }

    #[test]
    fn resolve_object_store_operator_and_path_rejects_local_path() {
        let cfg = crate::fs::object_store::ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        };

        let err = resolve_object_store_operator_and_path("/tmp/data.parquet", &cfg)
            .expect_err("local path should be rejected");
        assert!(err.contains("expected object-store path"), "{err}");
    }
}
