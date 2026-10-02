// Appended only to the scratch copy's proto/mod.rs. Actual production Codec,
// frame encoder and GoAway caller execute unchanged in the baseline. This is
// a private-path wire/limit oracle, not a public arbitrary-debug producer API,
// an original-allocation grant proof, a deployed connection or a deadline.
#[doc(hidden)]
pub mod m07_fixed_goaway_probe {
    use super::GoAway;
    use crate::codec::{Codec, UserError};
    use crate::frame::{self, Reason, StreamId};
    use bytes::Bytes;
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    struct Wire(Arc<Mutex<Vec<u8>>>);
    impl AsyncRead for Wire {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for Wire {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    pub fn run_case(debug_len: usize) -> (Vec<u8>, Option<io::ErrorKind>, bool) {
        let wire = Arc::new(Mutex::new(Vec::with_capacity(debug_len + 17)));
        // This wire-only probe does not claim funding. Original fixed writer
        // Vec/Core funding and deallocation have separate allocator probes.
        let owner = crate::SendFrameBuffer::new(65536, 16384, Bytes::new()).unwrap();
        let mut codec: Codec<_, Bytes> =
            Codec::with_frame_buffers(Wire(wire.clone()), None, Some(owner.bind().unwrap()), 16384);
        // Explicitly exercise large peer permission versus the local bound.
        codec.set_max_send_frame_size(16777215);
        let debug: Vec<u8> = (0..debug_len).map(|i| (i % 251) as u8).collect();
        let frame = frame::GoAway::with_debug_data(
            StreamId::from(1),
            Reason::PROTOCOL_ERROR,
            Bytes::from(debug),
        );
        let mut goaway = GoAway::new();
        goaway.go_away_now(frame);
        let mut cx = Context::from_waker(Waker::noop());
        let (kind, payload_too_big) = match goaway.send_pending_go_away(&mut cx, &mut codec) {
            Poll::Ready(Some(Ok(reason))) => {
                assert_eq!(reason, Reason::PROTOCOL_ERROR);
                match codec.flush(&mut cx) {
                    Poll::Ready(result) => result.unwrap(),
                    Poll::Pending => panic!("ready wire must flush the actual accepted GOAWAY"),
                }
                (None, false)
            }
            Poll::Ready(Some(Err(error))) => {
                let actual_user_error = error
                    .get_ref()
                    .and_then(|error| error.downcast_ref::<UserError>());
                (
                    error.kind().into(),
                    matches!(actual_user_error, Some(UserError::PayloadTooBig)),
                )
            }
            _ => panic!("actual pending GOAWAY caller must decide with empty ready codec"),
        };
        drop(goaway);
        drop(codec);
        drop(owner);
        let bytes = std::mem::take(&mut *wire.lock().unwrap());
        (bytes, kind, payload_too_big)
    }
}
