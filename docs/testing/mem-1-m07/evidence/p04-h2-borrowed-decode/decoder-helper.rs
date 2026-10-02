// Appended only inside the scratch dependency's actual framed_read module.
// No decoder algorithm is copied or replaced. Production h2 is a normal
// dependency, so its cfg(test) upstream fixture modules are not compiled.
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug)]
pub struct M07BorrowedDecoder {
    hpack: hpack::Decoder,
    partial: Option<Partial>,
    data_pool: Option<crate::ReceiveBufferPool>,
    goaway_pool: Option<crate::ReceiveBufferPool>,
}
#[doc(hidden)]
#[allow(missing_docs)]
#[derive(Debug, PartialEq, Eq)]
pub struct M07DecodedSummary {
    pub kind: Option<u8>,
    pub details: String,
    pub body: Option<bytes::Bytes>,
    pub stream: u32,
    pub end_stream: bool,
    pub error_class: Option<&'static str>,
    pub reason: Option<u32>,
}
#[allow(missing_docs)]
impl M07BorrowedDecoder {
    pub fn new(
        data_pool: Option<crate::ReceiveBufferPool>,
        goaway_pool: Option<crate::ReceiveBufferPool>,
    ) -> Self {
        Self {
            hpack: hpack::Decoder::new(4096),
            partial: None,
            data_pool: data_pool.map(|pool| pool.bind(16384).unwrap()),
            goaway_pool: goaway_pool.map(|pool| pool.bind(16384).unwrap()),
        }
    }
    pub fn decode(
        &mut self,
        wire: &[u8],
        owned: bool,
        begin: impl FnOnce(),
        end: impl FnOnce(),
    ) -> M07DecodedSummary {
        assert!(wire.len() >= frame::HEADER_LEN);
        begin();
        let input = if owned {
            FrameInput::Owned(BytesMut::from(wire))
        } else {
            FrameInput::Borrowed(wire)
        };
        let result = decode_frame_input(
            &mut self.hpack,
            16384,
            5,
            Some(16384),
            &mut self.partial,
            DecodePools {
                data: self.data_pool.as_ref(),
                goaway: self.goaway_pool.as_ref(),
            },
            input,
        );
        // Actual decoded Frame/Bytes remain owned while tracking ends. String
        // formatting, summary projection and metadata reads are outside it.
        end();
        let mut summary = M07DecodedSummary {
            kind: None,
            details: String::new(),
            body: None,
            stream: 0,
            end_stream: false,
            error_class: None,
            reason: None,
        };
        match result {
            Ok(Some(value)) => {
                summary.details = format!("{value:?}");
                match value {
                    Frame::Data(data) => {
                        summary.kind = Some(0);
                        summary.stream = data.stream_id().into();
                        summary.end_stream = data.is_end_stream();
                        summary.body = Some(data.into_payload());
                    }
                    Frame::Headers(_) => summary.kind = Some(1),
                    Frame::Priority(_) => summary.kind = Some(2),
                    Frame::Reset(_) => summary.kind = Some(3),
                    Frame::Settings(_) => summary.kind = Some(4),
                    Frame::PushPromise(_) => summary.kind = Some(5),
                    Frame::Ping(_) => summary.kind = Some(6),
                    Frame::GoAway(goaway) => {
                        summary.kind = Some(7);
                        summary.stream = goaway.last_stream_id().into();
                        summary.reason = Some(goaway.reason().into());
                        summary.body = Some(goaway.debug_data().clone());
                    }
                    Frame::WindowUpdate(_) => summary.kind = Some(8),
                }
            }
            Ok(None) => {}
            Err(error) => {
                summary.details = format!("{error:?}");
                match error {
                    Error::Reset(stream, reason, _) => {
                        summary.error_class = Some("Reset");
                        summary.stream = stream.into();
                        summary.reason = Some(reason.into());
                    }
                    Error::GoAway(_, reason, _) => {
                        summary.error_class = Some("GoAway");
                        summary.reason = Some(reason.into());
                    }
                    Error::Io(_, _) => summary.error_class = Some("Io"),
                }
            }
        }
        summary
    }
}
