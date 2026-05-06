//! Resolve the trailing `__nr_meta_<type>__` suffix that the parser-level
//! pre-parse rewrites `<tbl>$<metatype>` into.
//!
//! Mirrors `iceberg_ref::split_ref_suffix` for branch/tag.

use crate::connector::iceberg::IcebergMetadataTableType;

/// Inspect the trailing identifier part of a qualified name and, if it
/// matches `__nr_meta_<type>__`, return the parts with the suffix stripped
/// plus the parsed metadata-table type.
pub fn split_metadata_suffix(
    parts: &[String],
) -> (Vec<String>, Option<IcebergMetadataTableType>) {
    if let Some(last) = parts.last() {
        if let Some(inner) = last.strip_prefix("__nr_meta_").and_then(|s| s.strip_suffix("__")) {
            if let Ok(ty) = IcebergMetadataTableType::parse(inner) {
                return (
                    parts[..parts.len() - 1].to_vec(),
                    Some(ty),
                );
            }
        }
    }
    (parts.to_vec(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_suffix_is_stripped() {
        let parts = vec!["db".to_string(), "t".to_string(), "__nr_meta_snapshots__".to_string()];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(stripped, vec!["db".to_string(), "t".to_string()]);
        assert_eq!(ty, Some(IcebergMetadataTableType::Snapshots));
    }

    #[test]
    fn three_part_qualified_name_works() {
        let parts = vec![
            "ice".to_string(),
            "db".to_string(),
            "t".to_string(),
            "__nr_meta_history__".to_string(),
        ];
        let (stripped, ty) = split_metadata_suffix(&parts);
        assert_eq!(
            stripped,
            vec!["ice".to_string(), "db".to_string(), "t".to_string()]
        );
        assert_eq!(ty, Some(IcebergMetadataTableType::History));
    }

    #[test]
    fn refs_and_partitions_round_trip() {
        for (suffix, expected) in [
            ("__nr_meta_refs__", IcebergMetadataTableType::Refs),
            ("__nr_meta_partitions__", IcebergMetadataTableType::Partitions),
        ] {
            let parts = vec!["t".to_string(), suffix.to_string()];
            let (_, ty) = split_metadata_suffix(&parts);
            assert_eq!(ty, Some(expected));
        }
    }

    #[test]
    fn unrecognised_metatype_returns_none() {
        let parts = vec!["t".to_string(), "__nr_meta_xyz__".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }

    #[test]
    fn no_suffix_passthrough() {
        let parts = vec!["db".to_string(), "t".to_string()];
        let (out_parts, ty) = split_metadata_suffix(&parts);
        assert_eq!(out_parts, parts);
        assert_eq!(ty, None);
    }
}
