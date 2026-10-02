// Appended to the exact current receive_frame.rs in an isolated crate.
// These probes measure Rust-requested raw Vec/Core allocations. Frame copies,
// ownership-carrier metadata, transport/task metadata, libc/TLS and RSS are
// separate. ExitCredit below is a physical-drop oracle, not a second wallet.
#[cfg(test)]
mod borrowed_frame_probes {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize};

    const RAW: usize = 1;
    const CALLBACK: usize = 2;
    const CARRIER: usize = 3;
    const MAX_RECORDS: usize = 64;
    thread_local! { static MODE: Cell<usize> = const { Cell::new(0) }; }
    static POINTERS: [AtomicPtr<u8>; MAX_RECORDS] =
        [const { AtomicPtr::new(ptr::null_mut()) }; MAX_RECORDS];
    static ROLES: [AtomicU8; MAX_RECORDS] = [const { AtomicU8::new(0) }; MAX_RECORDS];
    static LIVE: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static REQUESTED: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static ALLOCATIONS: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static MAX_REQUEST: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];
    static OVERFLOW: AtomicBool = AtomicBool::new(false);
    struct Tracked;
    // SAFETY: All operations forward the unchanged allocator contract to System.
    // Fixed atomic records observe only allocations in the explicit TLS scope;
    // deallocation is recorded after the real System.dealloc has returned.
    unsafe impl GlobalAlloc for Tracked {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: Forward the requested allocation unchanged.
            let allocation = unsafe { System.alloc(layout) };
            let mode = MODE.try_with(Cell::get).unwrap_or(0);
            if mode != 0 && !allocation.is_null() {
                REQUESTED[mode].fetch_add(layout.size(), Ordering::AcqRel);
                ALLOCATIONS[mode].fetch_add(1, Ordering::AcqRel);
                MAX_REQUEST[mode].fetch_max(layout.size(), Ordering::AcqRel);
                let mut recorded = false;
                for (index, pointer) in POINTERS.iter().enumerate() {
                    if pointer
                        .compare_exchange(
                            ptr::null_mut(),
                            allocation,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        ROLES[index].store(mode as u8, Ordering::Release);
                        LIVE[mode].fetch_add(1, Ordering::AcqRel);
                        recorded = true;
                        break;
                    }
                }
                if !recorded {
                    OVERFLOW.store(true, Ordering::Release);
                }
            }
            allocation
        }
        unsafe fn dealloc(&self, allocation: *mut u8, layout: Layout) {
            // SAFETY: Forward the requested physical deallocation unchanged.
            unsafe { System.dealloc(allocation, layout) };
            for (index, pointer) in POINTERS.iter().enumerate() {
                if pointer
                    .compare_exchange(
                        allocation,
                        ptr::null_mut(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    let role = ROLES[index].swap(0, Ordering::AcqRel) as usize;
                    LIVE[role].fetch_sub(1, Ordering::AcqRel);
                    break;
                }
            }
        }
    }
    #[global_allocator]
    static ALLOCATOR: Tracked = Tracked;
    struct Tracking;
    impl Drop for Tracking {
        fn drop(&mut self) {
            MODE.with(|mode| mode.set(0));
        }
    }
    fn measured<T>(role: usize, action: impl FnOnce() -> T) -> T {
        MODE.with(|mode| {
            assert_eq!(mode.replace(role), 0);
        });
        let guard = Tracking;
        let result = action();
        drop(guard);
        result
    }
    fn reset() {
        assert!(
            !OVERFLOW.load(Ordering::Acquire),
            "allocation ledger overflowed"
        );
        for role in 1..4 {
            assert_eq!(
                LIVE[role].load(Ordering::Acquire),
                0,
                "prior backing still live"
            );
            REQUESTED[role].store(0, Ordering::Release);
            ALLOCATIONS[role].store(0, Ordering::Release);
            MAX_REQUEST[role].store(0, Ordering::Release);
        }
    }
    struct ExitCredit {
        exits: Arc<AtomicUsize>,
        funding: Arc<AtomicUsize>,
    }
    impl Drop for ExitCredit {
        fn drop(&mut self) {
            assert!(!OVERFLOW.load(Ordering::Acquire));
            assert_eq!(
                LIVE[RAW].load(Ordering::Acquire),
                0,
                "original credit exited before physical raw Vec/Core deallocation"
            );
            assert_eq!(
                LIVE[CARRIER].load(Ordering::Acquire),
                0,
                "credit exited before its separately covered ownership carrier allocation"
            );
            assert_ne!(self.funding.swap(0, Ordering::AcqRel), 0);
            assert_eq!(self.exits.fetch_add(1, Ordering::AcqRel), 0);
        }
    }
    fn buffer(max: usize) -> (ReceiveFrameBuffer, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        reset();
        let bound = ReceiveFrameBuffer::allocation_capacity_bound(max).unwrap();
        let exits = Arc::new(AtomicUsize::new(0));
        let funding = Arc::new(AtomicUsize::new(bound));
        let carrier = measured(CARRIER, || {
            Bytes::from_owner_with_exit_guard(
                Bytes::new(),
                ExitCredit {
                    exits: exits.clone(),
                    funding: funding.clone(),
                },
            )
        });
        let buffer = measured(RAW, || ReceiveFrameBuffer::new(max, carrier).unwrap());
        assert_eq!(
            ALLOCATIONS[RAW].load(Ordering::Acquire),
            2,
            "probe must observe the actual Vec and Arc backing"
        );
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert!(REQUESTED[RAW].load(Ordering::Acquire) <= bound);
        (buffer, exits, funding)
    }

    #[derive(Clone, Copy)]
    enum Step {
        Bytes(usize),
        Pending,
        Error,
    }
    #[derive(Clone, Copy, Default)]
    struct ReadCall {
        requested: usize,
        consumed: usize,
    }
    struct ScriptedRead {
        bytes: Vec<u8>,
        steps: Vec<Step>,
        at: usize,
        step: usize,
        calls: [ReadCall; 512],
        polls: usize,
    }
    impl ScriptedRead {
        fn new(bytes: Vec<u8>, steps: Vec<Step>) -> Self {
            Self {
                bytes,
                steps,
                at: 0,
                step: 0,
                calls: [ReadCall::default(); 512],
                polls: 0,
            }
        }
    }
    impl AsyncRead for ScriptedRead {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            target: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let call = self.polls;
            assert!(call < self.calls.len(), "unexpected read loop");
            self.polls += 1;
            self.calls[call].requested = target.remaining();
            let step = self
                .steps
                .get(self.step)
                .copied()
                .unwrap_or(Step::Bytes(usize::MAX));
            match step {
                Step::Pending => {
                    self.step += 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Step::Error => {
                    self.step += 1;
                    Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
                }
                Step::Bytes(available) => {
                    let count = available
                        .min(target.remaining())
                        .min(self.bytes.len() - self.at);
                    target.put_slice(&self.bytes[self.at..self.at + count]);
                    self.at += count;
                    self.calls[call].consumed = count;
                    if count == available {
                        self.step += 1;
                    } else if self.step < self.steps.len() {
                        let step_index = self.step;
                        self.steps[step_index] = Step::Bytes(available - count);
                    }
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
    fn frame(payload: &[u8]) -> Vec<u8> {
        let length = payload.len();
        let mut bytes = vec![
            (length >> 16) as u8,
            (length >> 8) as u8,
            length as u8,
            0,
            0,
            0,
            0,
            0,
            1,
        ];
        bytes.extend_from_slice(payload);
        bytes
    }
    fn reader(
        input: Vec<u8>,
        steps: Vec<Step>,
        max: usize,
    ) -> (
        FixedFrameRead<ScriptedRead>,
        ReceiveFrameBuffer,
        Arc<AtomicUsize>,
    ) {
        let (backing, exits, _) = buffer(max);
        let lease = backing.bind(max).unwrap();
        (
            FixedFrameRead::new(ScriptedRead::new(input, steps), lease, max),
            backing,
            exits,
        )
    }
    fn poll<R>(
        reader: &mut FixedFrameRead<ScriptedRead>,
        callback: impl FnOnce(&[u8]) -> R,
    ) -> Poll<Option<io::Result<R>>> {
        reader.poll_frame_with(&mut Context::from_waker(std::task::Waker::noop()), callback)
    }
    fn ready<R>(reader: &mut FixedFrameRead<ScriptedRead>, callback: impl FnOnce(&[u8]) -> R) -> R {
        match measured(CALLBACK, || poll(reader, callback)) {
            Poll::Ready(Some(Ok(value))) => value,
            _ => panic!("expected an exact complete borrowed frame"),
        }
    }
    fn no_callback_allocations() {
        assert_eq!(
            ALLOCATIONS[CALLBACK].load(Ordering::Acquire),
            0,
            "borrowed read/control callback allocated output backing"
        );
    }

    #[test]
    fn borrowed_pointer_is_original_backing_and_no_frame_copy_is_allocated() {
        let input = frame(b"original bytes");
        let (mut reader, backing, exits) = reader(input.clone(), Vec::new(), 16384);
        let storage = reader.buffer.storage().as_ptr();
        let length = ready(&mut reader, |bytes| {
            assert_eq!(bytes.as_ptr(), storage);
            assert_eq!(bytes, input.as_slice());
            bytes.len()
        });
        assert_eq!(length, input.len());
        no_callback_allocations();
        assert!(
            REQUESTED[RAW].load(Ordering::Acquire)
                <= ReceiveFrameBuffer::allocation_capacity_bound(16384).unwrap()
        );
        eprintln!(
            "raw requested={} bound={} successful callback allocations=0",
            REQUESTED[RAW].load(Ordering::Acquire),
            ReceiveFrameBuffer::allocation_capacity_bound(16384).unwrap()
        );
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        drop(reader);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn repeated_empty_padded_control_and_full_frames_reuse_the_same_storage() {
        let mut empty = frame(&[]);
        empty[3] = 4; // SETTINGS ACK, zero payload.
        empty[4] = 1;
        empty[8] = 0;
        let mut padded = frame(&[2, b'x', b'y', 0, 0]);
        padded[4] = 8; // DATA PADDED; raw framing leaves payload semantics to decoder.
        let mut ping = frame(b"12345678");
        ping[3] = 6;
        ping[8] = 0;
        let full = frame(&vec![0xab; 16384]);
        let frames = [empty, padded, ping, full];
        let mut wire = Vec::new();
        for _ in 0..32 {
            for bytes in &frames {
                wire.extend_from_slice(bytes);
            }
        }
        let (mut reader, backing, exits) = reader(wire, Vec::new(), 16384);
        let storage = reader.buffer.storage().as_ptr();
        let mut consumed = 0;
        for _ in 0..32 {
            for expected in &frames {
                ready(&mut reader, |bytes| {
                    assert_eq!(bytes.as_ptr(), storage);
                    assert_eq!(bytes, expected);
                });
                consumed += expected.len();
                assert_eq!(
                    reader.io.at, consumed,
                    "reader consumed bytes from the next frame"
                );
            }
        }
        no_callback_allocations();
        assert!(matches!(
            measured(CALLBACK, || poll(&mut reader, |_| panic!(
                "callback on clean EOF"
            ))),
            Poll::Ready(None)
        ));
        no_callback_allocations();
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn partial_header_body_and_pending_preserve_position_without_callback() {
        let first = frame(b"abcdef");
        let second = frame(b"next");
        let mut wire = first.clone();
        wire.extend_from_slice(&second);
        let steps = vec![
            Step::Bytes(2),
            Step::Pending,
            Step::Bytes(7),
            Step::Bytes(2),
            Step::Pending,
            Step::Bytes(4),
        ];
        let (mut reader, backing, exits) = reader(wire, steps, 16384);
        for consumed in [2, 11] {
            assert!(measured(CALLBACK, || poll(&mut reader, |_| panic!(
                "partial callback"
            )))
            .is_pending());
            assert_eq!(reader.io.at, consumed);
        }
        ready(&mut reader, |bytes| assert_eq!(bytes, first));
        assert_eq!(reader.io.at, first.len());
        ready(&mut reader, |bytes| assert_eq!(bytes, second));
        for call in reader.io.calls.iter().take(reader.io.polls) {
            assert!(call.consumed <= call.requested);
        }
        no_callback_allocations();
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn callback_panic_resets_frame_before_unwind_and_next_frame_is_not_replayed() {
        let first = frame(b"discarded");
        let next = frame(b"following");
        let mut wire = first.clone();
        wire.extend_from_slice(&next);
        let (mut reader, backing, exits) = reader(wire, Vec::new(), 16384);
        // Panic runtime allocations are outside the callback-allocation oracle.
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = poll(&mut reader, |bytes| {
                assert_eq!(bytes, first);
                panic!("injected callback failure");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(reader.filled, 0);
        assert_eq!(reader.target, 9);
        assert_eq!(reader.io.at, first.len());
        ready(&mut reader, |bytes| assert_eq!(bytes, next));
        no_callback_allocations();
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn all_truncated_header_and_body_lengths_refuse_without_callback_or_replay() {
        let complete = frame(b"payload");
        for length in 0..complete.len() {
            let (mut reader, backing, exits) =
                reader(complete[..length].to_vec(), Vec::new(), 16384);
            let result = poll(&mut reader, |_| panic!("truncated frame callback"));
            if length == 0 {
                assert!(matches!(result, Poll::Ready(None)));
            } else {
                match result {
                    Poll::Ready(Some(Err(error))) => {
                        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof)
                    }
                    _ => panic!("missing truncation error"),
                }
            }
            let calls = reader.io.polls;
            assert!(matches!(
                poll(&mut reader, |_| panic!("replayed after failure")),
                Poll::Ready(None)
            ));
            assert_eq!(reader.io.polls, calls);
            drop(reader);
            drop(backing);
            assert_eq!(exits.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn oversized_header_refuses_before_payload_read_or_callback() {
        let mut wire = frame(b"not consumed");
        wire[..3].copy_from_slice(&[0, 64, 1]); // 16385 > local 16384.
        let (mut reader, backing, exits) = reader(wire, Vec::new(), 16384);
        let result = measured(CALLBACK, || {
            poll(&mut reader, |_| panic!("oversized callback"))
        });
        match result {
            Poll::Ready(Some(Err(error))) => {
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert!(error.get_ref().unwrap().is::<FrameSizeExceeded>());
            }
            _ => panic!("missing frame-size error"),
        }
        assert_eq!(reader.io.at, 9);
        assert_eq!(reader.io.polls, 1);
        // io::Error's independent wrapper may allocate; no body-sized copy or
        // callback output is attributed to the original raw grant.
        assert!(MAX_REQUEST[CALLBACK].load(Ordering::Acquire) < 16384);
        let calls = reader.io.polls;
        assert!(matches!(
            poll(&mut reader, |_| panic!("failure replay")),
            Poll::Ready(None)
        ));
        assert_eq!(reader.io.polls, calls);
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn local_maximum_change_applies_before_callback_and_does_not_grow_backing() {
        let exact = frame(&vec![1; 16384]);
        let too_large = frame(&vec![2; 16385]);
        let mut wire = exact.clone();
        wire.extend_from_slice(&too_large);
        let (mut reader, backing, exits) = reader(wire, Vec::new(), 65536);
        reader.set_max_payload(16384);
        ready(&mut reader, |bytes| assert_eq!(bytes, exact));
        no_callback_allocations();
        assert!(matches!(
            poll(&mut reader, |_| panic!("lowered maximum bypass")),
            Poll::Ready(Some(Err(_)))
        ));
        assert_eq!(reader.io.at, exact.len() + 9);
        assert_eq!(ALLOCATIONS[RAW].load(Ordering::Acquire), 2);
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn read_error_is_terminal_and_never_invokes_callback() {
        let (mut reader, backing, exits) =
            reader(frame(b"body"), vec![Step::Bytes(3), Step::Error], 16384);
        match poll(&mut reader, |_| panic!("I/O failure callback")) {
            Poll::Ready(Some(Err(error))) => {
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset)
            }
            _ => panic!("missing I/O error"),
        }
        let calls = reader.io.polls;
        assert!(matches!(
            poll(&mut reader, |_| panic!("I/O failure replay")),
            Poll::Ready(None)
        ));
        assert_eq!(reader.io.polls, calls);
        drop(reader);
        drop(backing);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }
}
