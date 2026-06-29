pub(crate) fn stable_join_row_key(
    left_uuid: &str,
    left_row_id: i64,
    right_uuid: &str,
    right_row_id: i64,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(left_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(left_row_id.to_be_bytes());
    hasher.update([0]);
    hasher.update(right_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(right_row_id.to_be_bytes());
    let digest = hasher.finalize();
    format!("v1:{}", hex::encode(&digest[..16]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_join_row_key_is_deterministic() {
        let first = stable_join_row_key("left-uuid", 11, "right-uuid", 22);
        let second = stable_join_row_key("left-uuid", 11, "right-uuid", 22);

        assert_eq!(first, second);
        assert!(first.starts_with("v1:"));
        assert_eq!(first.len(), "v1:".len() + 32);
    }

    #[test]
    fn stable_join_row_key_distinguishes_row_identity() {
        let base = stable_join_row_key("left-uuid", 11, "right-uuid", 22);

        assert_ne!(
            base,
            stable_join_row_key("other-left", 11, "right-uuid", 22)
        );
        assert_ne!(base, stable_join_row_key("left-uuid", 12, "right-uuid", 22));
        assert_ne!(
            base,
            stable_join_row_key("left-uuid", 11, "other-right", 22)
        );
        assert_ne!(base, stable_join_row_key("left-uuid", 11, "right-uuid", 23));
    }
}
