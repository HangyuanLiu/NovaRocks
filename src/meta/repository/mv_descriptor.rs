use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const MV_DESCRIPTOR_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorDependency {
    pub catalog: String,
    pub namespace: String,
    pub name: String,
    pub object_type: String,
    pub storage_engine: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvDescriptorV1 {
    pub descriptor_version: u16,
    pub package_id: String,
    pub logical_sql: String,
    pub dialect: String,
    pub public_view: String,
    pub storage_table: String,
    pub visible_columns: Vec<String>,
    pub hidden_columns: Vec<String>,
    pub base_dependencies: Vec<DescriptorDependency>,
    #[serde(default)]
    pub schema_contract: Option<Value>,
    #[serde(default)]
    pub refresh_contract: Option<Value>,
    pub created_at_ms: i64,
}

impl MvDescriptorV1 {
    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV descriptor: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV descriptor JSON: {err}"))
    }

    pub fn content_hash(&self) -> Result<String, String> {
        let canonical_json = self.to_canonical_json()?;
        Ok(hex_encode(&Sha256::digest(canonical_json.as_bytes())))
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        let descriptor: Self = serde_json::from_str(s)
            .map_err(|err| format!("failed to parse MV descriptor JSON: {err}"))?;
        if descriptor.descriptor_version != MV_DESCRIPTOR_VERSION {
            return Err(format!(
                "unsupported MV descriptor version: expected {}, got {}",
                MV_DESCRIPTOR_VERSION, descriptor.descriptor_version
            ));
        }
        Ok(descriptor)
    }
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, sort_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> MvDescriptorV1 {
        MvDescriptorV1 {
            descriptor_version: MV_DESCRIPTOR_VERSION,
            package_id: "pkg-1".to_string(),
            logical_sql: "SELECT id FROM ice.sales.orders".to_string(),
            dialect: "starrocks".to_string(),
            public_view: "analytics.mv_orders".to_string(),
            storage_table: "analytics.__nr_mv_mv_orders".to_string(),
            visible_columns: vec!["id".to_string()],
            hidden_columns: vec!["__nova_base_row_id".to_string()],
            base_dependencies: vec![DescriptorDependency {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: "table".to_string(),
                storage_engine: "iceberg".to_string(),
            }],
            schema_contract: Some(serde_json::json!({
                "z": 3,
                "a": {
                    "z": 2,
                    "a": 1
                }
            })),
            refresh_contract: None,
            created_at_ms: 123,
        }
    }

    #[test]
    fn descriptor_json_round_trips() {
        let descriptor = sample();
        let json = descriptor.to_canonical_json().unwrap();

        let parsed = MvDescriptorV1::from_json(&json).unwrap();

        assert_eq!(parsed, descriptor);
    }

    #[test]
    fn descriptor_json_rejects_unsupported_version() {
        let mut descriptor = sample();
        descriptor.descriptor_version = MV_DESCRIPTOR_VERSION + 1;
        let json = descriptor.to_canonical_json().unwrap();

        let err = MvDescriptorV1::from_json(&json).unwrap_err();

        assert!(err.contains("unsupported MV descriptor version"));
        assert!(err.contains(&(MV_DESCRIPTOR_VERSION + 1).to_string()));
    }

    #[test]
    fn canonical_json_is_key_sorted_and_hash_stable() {
        let descriptor = sample();

        let canonical = descriptor.to_canonical_json().unwrap();

        assert_eq!(
            canonical,
            "{\"base_dependencies\":[{\"catalog\":\"ice\",\"name\":\"orders\",\"namespace\":\"sales\",\"object_type\":\"table\",\"storage_engine\":\"iceberg\"}],\"created_at_ms\":123,\"descriptor_version\":1,\"dialect\":\"starrocks\",\"hidden_columns\":[\"__nova_base_row_id\"],\"logical_sql\":\"SELECT id FROM ice.sales.orders\",\"package_id\":\"pkg-1\",\"public_view\":\"analytics.mv_orders\",\"refresh_contract\":null,\"schema_contract\":{\"a\":{\"a\":1,\"z\":2},\"z\":3},\"storage_table\":\"analytics.__nr_mv_mv_orders\",\"visible_columns\":[\"id\"]}"
        );
        assert_eq!(
            descriptor.content_hash().unwrap(),
            "05707bad24830c2246f225b7bf59e3d8b2a8b79ebbb53eb938cd4fc9087f2802"
        );
    }
}
