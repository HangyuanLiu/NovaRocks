// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reuse the SDK footer decoder with an immutable inline length. All reads
//! retain the original admitted input; this grants no filesystem authority.

use crate::iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage};
use crate::iceberg::{Error, ErrorKind, Result};
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::Arc;

#[derive(Debug)]
struct KnownLengthInput {
    original: InputFile,
    length: u64,
}
impl serde::Serialize for KnownLengthInput {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "Reclamation input capabilities cannot be serialized",
        ))
    }
}
impl<'de> serde::Deserialize<'de> for KnownLengthInput {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "Reclamation input capabilities cannot be deserialized",
        ))
    }
}
impl KnownLengthInput {
    fn check(&self, path: &str) -> Result<()> {
        if path == self.original.location() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::DataInvalid,
                "Known-length input does not authorize another path",
            ))
        }
    }
}
fn unsupported<T>() -> Result<T> {
    Err(Error::new(
        ErrorKind::FeatureUnsupported,
        "Known-length reclamation input only supports exact-path reads",
    ))
}
#[async_trait::async_trait]
#[typetag::serde(name = "IcebergReclamationKnownLengthInput")]
impl Storage for KnownLengthInput {
    async fn exists(&self, _: &str) -> Result<bool> {
        unsupported()
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.check(path)?;
        Ok(FileMetadata { size: self.length })
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.check(path)?;
        self.original.read().await
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.check(path)?;
        self.original.reader().await
    }
    async fn write(&self, _: &str, _: Bytes) -> Result<()> {
        unsupported()
    }
    async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
        unsupported()
    }
    async fn delete(&self, _: &str) -> Result<()> {
        unsupported()
    }
    async fn delete_prefix(&self, _: &str) -> Result<()> {
        unsupported()
    }
    async fn delete_stream(&self, _: BoxStream<'static, String>) -> Result<()> {
        unsupported()
    }
    fn new_input(&self, _: &str) -> Result<InputFile> {
        unsupported()
    }
    fn new_output(&self, _: &str) -> Result<OutputFile> {
        unsupported()
    }
}
pub(super) fn bind(original: InputFile, length: u64) -> InputFile {
    let path = original.location().to_owned();
    InputFile::new(Arc::new(KnownLengthInput { original, length }), path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct HeadSpy {
        original: InputFile,
        heads: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
    }
    impl serde::Serialize for HeadSpy {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("Test capability is not portable"))
        }
    }
    impl<'de> serde::Deserialize<'de> for HeadSpy {
        fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
            Err(serde::de::Error::custom("Test capability is not portable"))
        }
    }
    #[async_trait::async_trait]
    #[typetag::serde(name = "IcebergReclamationHeadSpy")]
    impl Storage for HeadSpy {
        async fn exists(&self, _: &str) -> Result<bool> {
            unsupported()
        }
        async fn metadata(&self, _: &str) -> Result<FileMetadata> {
            self.heads.fetch_add(1, Ordering::SeqCst);
            self.original.metadata().await
        }
        async fn read(&self, _: &str) -> Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.original.read().await
        }
        async fn reader(&self, _: &str) -> Result<Box<dyn FileRead>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.original.reader().await
        }
        async fn write(&self, _: &str, _: Bytes) -> Result<()> {
            unsupported()
        }
        async fn writer(&self, _: &str) -> Result<Box<dyn FileWrite>> {
            unsupported()
        }
        async fn delete(&self, _: &str) -> Result<()> {
            unsupported()
        }
        async fn delete_prefix(&self, _: &str) -> Result<()> {
            unsupported()
        }
        async fn delete_stream(&self, _: BoxStream<'static, String>) -> Result<()> {
            unsupported()
        }
        fn new_input(&self, _: &str) -> Result<InputFile> {
            unsupported()
        }
        fn new_output(&self, _: &str) -> Result<OutputFile> {
            unsupported()
        }
    }
    #[tokio::test]
    async fn actual_sdk_footer_decoder_reads_original_input_with_zero_head() {
        let io = crate::iceberg::io::FileIO::new_with_memory();
        let path = "memory://gc/empty.puffin";
        // Independent Puffin framing, no production writer/helper oracle.
        let payload = br#"{"blobs":[]}"#;
        let mut bytes = b"PFA1PFA1".to_vec();
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(b"PFA1");
        io.new_output(path)
            .unwrap()
            .write(Bytes::from(bytes.clone()))
            .await
            .unwrap();
        let heads = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let spy = InputFile::new(
            Arc::new(HeadSpy {
                original: io.new_input(path).unwrap(),
                heads: heads.clone(),
                reads: reads.clone(),
            }),
            path.into(),
        );
        let reader = crate::iceberg::puffin::PuffinReader::new(bind(spy, bytes.len() as u64))
            .await
            .unwrap();
        assert!(reader.file_metadata().await.unwrap().blobs().is_empty());
        assert_eq!(heads.load(Ordering::SeqCst), 0);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn exact_path_and_process_local_serialization_fail_closed() {
        let io = crate::iceberg::io::FileIO::new_with_memory();
        let storage: Arc<dyn Storage> = Arc::new(KnownLengthInput {
            original: io.new_input("memory://gc/one").unwrap(),
            length: 20,
        });
        assert!(storage.metadata("memory://gc/other").await.is_err());
        assert!(storage.reader("memory://gc/other").await.is_err());
        assert!(
            storage
                .write("memory://gc/one", Bytes::new())
                .await
                .is_err()
        );
        assert!(serde_json::to_vec(&storage).is_err());
        assert!(
            serde_json::from_str::<Box<dyn Storage>>(
                r#"{"type":"IcebergReclamationKnownLengthInput"}"#
            )
            .is_err()
        );
    }
}
