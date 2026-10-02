// Appended to the unmodified current receive_frame.rs in an isolated crate.
// These probes measure Rust-requested raw Vec/Core allocations. Frame copies,
// ownership-carrier metadata, transport/task metadata, libc/TLS and RSS are
// separate. ExitCredit below is a physical-drop oracle, not a second wallet.
#[cfg(test)]
mod raw_input_probes {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize};
    use std::sync::Barrier;

    const RAW: usize = 1;
    const FRAME: usize = 2;
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
        calls: [ReadCall; 64],
        polls: usize,
    }
    impl ScriptedRead {
        fn new(bytes: Vec<u8>, steps: Vec<Step>) -> Self {
            Self {
                bytes,
                steps,
                at: 0,
                step: 0,
                calls: [ReadCall::default(); 64],
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
    fn poll(reader: &mut FixedFrameRead<ScriptedRead>) -> Poll<Option<io::Result<BytesMut>>> {
        reader.poll_frame(&mut Context::from_waker(std::task::Waker::noop()))
    }
    fn ready_frame(reader: &mut FixedFrameRead<ScriptedRead>) -> BytesMut {
        match poll(reader) {
            Poll::Ready(Some(Ok(bytes))) => bytes,
            _ => panic!("expected a complete frame"),
        }
    }
    fn error(reader: &mut FixedFrameRead<ScriptedRead>, kind: io::ErrorKind) -> io::Error {
        match poll(reader) {
            Poll::Ready(Some(Err(error))) => {
                assert_eq!(error.kind(), kind);
                error
            }
            _ => panic!("expected the specific frame input failure"),
        }
    }

    #[test]
    fn actual_vec_and_core_requests_fit_bound_without_clone_or_bind_allocations() {
        for max in [16_384, 65_536] {
            let (buffer, exits, funding) = buffer(max);
            eprintln!(
                "raw payload={max} requested={} bound={} allocations={}",
                REQUESTED[RAW].load(Ordering::Acquire),
                ReceiveFrameBuffer::allocation_capacity_bound(max).unwrap(),
                ALLOCATIONS[RAW].load(Ordering::Acquire)
            );
            let before = ALLOCATIONS[RAW].load(Ordering::Acquire);
            let clone = measured(RAW, || buffer.clone());
            let lease = measured(RAW, || clone.bind(max).unwrap());
            assert_eq!(ALLOCATIONS[RAW].load(Ordering::Acquire), before);
            assert_eq!(buffer.max_payload_bytes(), max);
            drop(buffer);
            drop(clone);
            assert_eq!(exits.load(Ordering::Acquire), 0);
            assert_ne!(funding.load(Ordering::Acquire), 0);
            drop(lease);
            assert_eq!(exits.load(Ordering::Acquire), 1);
            assert_eq!(funding.load(Ordering::Acquire), 0);
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn maximum_u24_geometry_has_the_same_two_fixed_allocations() {
        let (buffer, exits, _) = buffer(16_777_215);
        assert_eq!(MAX_REQUEST[RAW].load(Ordering::Acquire), 16_777_224);
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn opaque_clones_retain_credit_until_last_physical_backing_exit() {
        let (buffer, exits, funding) = buffer(16_384);
        let first = buffer.clone();
        let last = first.clone();
        drop(buffer);
        drop(first);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_ne!(funding.load(Ordering::Acquire), 0);
        drop(last);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(funding.load(Ordering::Acquire), 0);
    }

    #[test]
    fn over_capacity_bind_does_not_consume_the_once_only_lease() {
        let (buffer, exits, _) = buffer(16_384);
        assert_eq!(
            buffer.bind(16_385).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        let lease = buffer.bind(16_384).unwrap();
        assert!(buffer.bind(16_384).is_err());
        drop(lease);
        assert!(
            buffer.bind(16_384).is_err(),
            "dropping the lease must not permit rebinding"
        );
        drop(buffer);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn racing_bind_mints_one_exclusive_lease_and_racing_last_drop_exits_once() {
        let (buffer, exits, _) = buffer(16_384);
        let barrier = Arc::new(Barrier::new(3));
        let left = buffer.clone();
        let right = buffer.clone();
        let left_gate = barrier.clone();
        let right_gate = barrier.clone();
        let left = std::thread::spawn(move || {
            left_gate.wait();
            left.bind(16_384)
        });
        let right = std::thread::spawn(move || {
            right_gate.wait();
            right.bind(16_384)
        });
        barrier.wait();
        let left = left.join().unwrap();
        let right = right.join().unwrap();
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let mut lease = left.or(right).unwrap();
        // Actual UnsafeCell access is exercised sequentially through the only
        // minted mutable lease, including after moving it between threads.
        lease.storage()[0] = 0x5a;
        assert_eq!(lease.storage()[0], 0x5a);
        let extra = buffer.clone();
        drop(buffer);
        let barrier = Arc::new(Barrier::new(3));
        let first = barrier.clone();
        let second = barrier.clone();
        let left = std::thread::spawn(move || {
            first.wait();
            drop(lease);
        });
        let right = std::thread::spawn(move || {
            second.wait();
            drop(extra);
        });
        assert_eq!(exits.load(Ordering::Acquire), 0);
        barrier.wait();
        left.join().unwrap();
        right.join().unwrap();
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn clean_eof_zero_frames_and_frame_boundaries_never_read_ahead() {
        let zero = frame(&[]);
        let payload = frame(b"second");
        let mut input = zero.clone();
        input.extend_from_slice(&payload);
        let script = ScriptedRead::new(input, vec![]);
        let (buffer, exits, _) = buffer(16_384);
        let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
        drop(buffer);
        assert_eq!(&ready_frame(&mut reader)[..], &zero);
        assert_eq!(reader.get_ref().at, 9);
        assert_eq!(reader.get_ref().polls, 1);
        assert_eq!(reader.get_ref().calls[0].requested, 9);
        assert_eq!(reader.get_ref().calls[0].consumed, 9);
        assert_eq!(&ready_frame(&mut reader)[..], &payload);
        assert_eq!(reader.get_ref().at, zero.len() + payload.len());
        assert_eq!(reader.get_ref().calls[1].requested, 9);
        assert_eq!(reader.get_ref().calls[2].requested, 6);
        assert!(matches!(poll(&mut reader), Poll::Ready(None)));
        let polls = reader.get_ref().polls;
        assert!(matches!(poll(&mut reader), Poll::Ready(None)));
        assert_eq!(reader.get_ref().polls, polls);
        drop(reader);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn pending_partial_header_and_body_resume_without_replaying_bytes() {
        let expected = frame(b"abcde");
        let script = ScriptedRead::new(
            expected.clone(),
            vec![
                Step::Bytes(3),
                Step::Pending,
                Step::Bytes(6),
                Step::Pending,
                Step::Bytes(2),
                Step::Pending,
                Step::Bytes(3),
            ],
        );
        let (buffer, exits, _) = buffer(16_384);
        let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
        drop(buffer);
        assert!(measured(FRAME, || poll(&mut reader)).is_pending());
        assert_eq!(reader.get_ref().at, 3);
        assert!(measured(FRAME, || poll(&mut reader)).is_pending());
        assert_eq!(reader.get_ref().at, 9);
        assert!(measured(FRAME, || poll(&mut reader)).is_pending());
        assert_eq!(reader.get_ref().at, 11);
        assert_eq!(
            ALLOCATIONS[FRAME].load(Ordering::Acquire),
            0,
            "incomplete frames must not allocate a frame copy"
        );
        let output = measured(FRAME, || ready_frame(&mut reader));
        assert_eq!(&output[..], &expected);
        assert_eq!(reader.get_ref().at, 14);
        assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 1);
        assert_eq!(REQUESTED[FRAME].load(Ordering::Acquire), expected.len());
        // The independently funded output copy may outlive raw input funding.
        drop(reader);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(LIVE[FRAME].load(Ordering::Acquire), 1);
        assert_eq!(&output[9..], b"abcde");
        drop(output);
    }

    #[test]
    fn every_partial_header_eof_is_sticky_and_never_allocates_a_frame_copy() {
        let expected = frame(b"abc");
        for length in 0..9 {
            let script = ScriptedRead::new(expected[..length].to_vec(), vec![]);
            let (buffer, exits, _) = buffer(16_384);
            let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
            drop(buffer);
            if length == 0 {
                assert!(matches!(
                    measured(FRAME, || poll(&mut reader)),
                    Poll::Ready(None)
                ));
                assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 0);
            } else {
                let failure = measured(FRAME, || error(&mut reader, io::ErrorKind::UnexpectedEof));
                assert!(
                    MAX_REQUEST[FRAME].load(Ordering::Acquire) <= 256,
                    "only the small error diagnostic is allowed on truncated input"
                );
                drop(failure);
            }
            assert_eq!(reader.get_ref().at, length);
            let polls = reader.get_ref().polls;
            assert!(matches!(poll(&mut reader), Poll::Ready(None)));
            assert_eq!(reader.get_ref().polls, polls);
            drop(reader);
            assert_eq!(exits.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn every_partial_body_eof_is_sticky_without_copying_the_partial_payload() {
        let expected = frame(b"abcd");
        for body in 0..4 {
            let script = ScriptedRead::new(expected[..9 + body].to_vec(), vec![]);
            let (buffer, exits, _) = buffer(16_384);
            let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
            drop(buffer);
            let failure = measured(FRAME, || error(&mut reader, io::ErrorKind::UnexpectedEof));
            assert!(MAX_REQUEST[FRAME].load(Ordering::Acquire) <= 256);
            drop(failure);
            assert_eq!(reader.get_ref().at, 9 + body);
            let polls = reader.get_ref().polls;
            assert!(matches!(poll(&mut reader), Poll::Ready(None)));
            assert_eq!(reader.get_ref().polls, polls);
            drop(reader);
            assert_eq!(exits.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn oversized_u24_header_refuses_before_body_read_or_payload_sized_allocation() {
        for payload in [16_385, 65_536, 16_777_215] {
            let mut input = vec![
                (payload >> 16) as u8,
                (payload >> 8) as u8,
                payload as u8,
                0,
                0,
                0,
                0,
                0,
                1,
            ];
            // The body is deliberately present: refusing to consume it is
            // checked using the actual AsyncRead cursor, not parser counters.
            input.extend_from_slice(b"do not read this body");
            let script = ScriptedRead::new(input, vec![]);
            let (buffer, exits, _) = buffer(16_384);
            let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
            drop(buffer);
            let failure = measured(FRAME, || error(&mut reader, io::ErrorKind::InvalidData));
            assert!(failure.get_ref().unwrap().is::<FrameSizeExceeded>());
            assert_eq!(reader.get_ref().at, 9);
            assert_eq!(reader.get_ref().polls, 1);
            assert_eq!(
                ALLOCATIONS[FRAME].load(Ordering::Acquire),
                1,
                "only the actual io::Error wrapper may allocate; no BytesMut copy"
            );
            assert!(
                MAX_REQUEST[FRAME].load(Ordering::Acquire) <= 256,
                "small io::Error backing is separate; no frame/payload allocation is allowed"
            );
            eprintln!(
                "oversized header payload={payload} consumed=9 diagnostic_requested={}",
                REQUESTED[FRAME].load(Ordering::Acquire)
            );
            drop(failure);
            assert!(matches!(poll(&mut reader), Poll::Ready(None)));
            assert_eq!(reader.get_ref().polls, 1);
            drop(reader);
            assert_eq!(exits.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn exact_local_maximum_frame_copy_is_separate_and_raw_capacity_never_grows() {
        let payload = vec![0x39; 16_384];
        let expected = frame(&payload);
        let script = ScriptedRead::new(expected.clone(), vec![]);
        let (buffer, exits, _) = buffer(16_384);
        let initial_raw_bytes = REQUESTED[RAW].load(Ordering::Acquire);
        let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
        drop(buffer);
        let output = measured(FRAME, || ready_frame(&mut reader));
        assert_eq!(&output[..], &expected);
        assert_eq!(REQUESTED[FRAME].load(Ordering::Acquire), 16_393);
        assert_eq!(REQUESTED[RAW].load(Ordering::Acquire), initial_raw_bytes);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert_eq!(reader.get_ref().polls, 2);
        assert_eq!(reader.get_ref().calls[0].requested, 9);
        assert_eq!(reader.get_ref().calls[1].requested, 16_384);
        drop(reader);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(LIVE[FRAME].load(Ordering::Acquire), 1);
        drop(output);
    }

    #[test]
    fn local_limit_change_reuses_backing_and_partial_io_error_is_terminal() {
        let expected = frame(b"abcdef");
        let script = ScriptedRead::new(expected, vec![Step::Bytes(9), Step::Bytes(2), Step::Error]);
        let (buffer, exits, _) = buffer(16_384);
        let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
        drop(buffer);
        reader.set_max_payload(6);
        assert_eq!(reader.max_payload(), 6);
        assert_eq!(reader.get_mut().at, 0);
        let failure = measured(FRAME, || error(&mut reader, io::ErrorKind::ConnectionReset));
        assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 0);
        assert_eq!(reader.get_ref().at, 11);
        drop(failure);
        let polls = reader.get_ref().polls;
        assert!(matches!(poll(&mut reader), Poll::Ready(None)));
        assert_eq!(reader.get_ref().polls, polls);
        drop(reader);
        assert_eq!(exits.load(Ordering::Acquire), 1);
    }

    #[test]
    fn cancelling_a_pending_read_releases_only_after_opaque_aliases_exit() {
        let script = ScriptedRead::new(frame(b"abc"), vec![Step::Bytes(4), Step::Pending]);
        let (buffer, exits, funding) = buffer(16_384);
        let alias = buffer.clone();
        let mut reader = FixedFrameRead::new(script, buffer.bind(16_384).unwrap(), 16_384);
        drop(buffer);
        assert!(measured(FRAME, || poll(&mut reader)).is_pending());
        drop(reader);
        assert_eq!(ALLOCATIONS[FRAME].load(Ordering::Acquire), 0);
        assert_eq!(LIVE[RAW].load(Ordering::Acquire), 2);
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_ne!(funding.load(Ordering::Acquire), 0);
        drop(alias);
        assert_eq!(exits.load(Ordering::Acquire), 1);
        assert_eq!(funding.load(Ordering::Acquire), 0);
    }

    #[test]
    fn invalid_geometry_never_requests_raw_payload_backing() {
        reset();
        for max in [0, 16_383, 16_777_216, usize::MAX] {
            assert_eq!(
                ReceiveFrameBuffer::allocation_capacity_bound(max)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
            let error = measured(RAW, || {
                ReceiveFrameBuffer::new(max, Bytes::new()).err().unwrap()
            });
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(
                MAX_REQUEST[RAW].load(Ordering::Acquire) <= 256,
                "only fixed diagnostics may allocate before invalid geometry refusal"
            );
            drop(error);
            assert_eq!(LIVE[RAW].load(Ordering::Acquire), 0);
        }
    }
}
