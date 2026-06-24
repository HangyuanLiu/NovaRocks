use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::env;
use std::hint::{black_box, spin_loop};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct Config {
    threads: usize,
    tasks: usize,
    iterations: usize,
    work_us: Vec<u64>,
    state_lines: usize,
    levels: usize,
    capacity: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let default_threads = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let mut cfg = Self {
            threads: default_threads,
            tasks: default_threads * 64,
            iterations: 200_000,
            work_us: vec![0, 50, 500],
            state_lines: 8,
            levels: 8,
            capacity: 0,
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--threads" => cfg.threads = parse_next(&mut args, "--threads")?,
                "--tasks" => cfg.tasks = parse_next(&mut args, "--tasks")?,
                "--iterations" => cfg.iterations = parse_next(&mut args, "--iterations")?,
                "--work-us" => {
                    let raw: String = parse_next(&mut args, "--work-us")?;
                    cfg.work_us = parse_list(&raw)?;
                }
                "--state-lines" => cfg.state_lines = parse_next(&mut args, "--state-lines")?,
                "--levels" => cfg.levels = parse_next(&mut args, "--levels")?,
                "--capacity" => cfg.capacity = parse_next(&mut args, "--capacity")?,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        cfg.threads = cfg.threads.max(1);
        cfg.tasks = cfg.tasks.max(cfg.threads);
        cfg.iterations = cfg.iterations.max(1);
        cfg.levels = cfg.levels.max(1);
        if cfg.capacity == 0 {
            cfg.capacity = (cfg.tasks * 2).next_power_of_two();
        }
        if !cfg.capacity.is_power_of_two() {
            return Err("--capacity must be a power of two".to_string());
        }
        if cfg.capacity <= cfg.tasks {
            return Err("--capacity must be greater than --tasks".to_string());
        }
        Ok(cfg)
    }
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    let raw = args
        .next()
        .ok_or_else(|| format!("{name} expects a value"))?;
    raw.parse::<T>()
        .map_err(|_| format!("invalid {name} value: {raw}"))
}

fn parse_list(raw: &str) -> Result<Vec<u64>, String> {
    let mut out = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(
            trimmed
                .parse::<u64>()
                .map_err(|_| format!("invalid --work-us item: {trimmed}"))?,
        );
    }
    if out.is_empty() {
        return Err("--work-us must contain at least one value".to_string());
    }
    Ok(out)
}

fn print_help() {
    println!("driver_scheduler_e0_bench");
    println!();
    println!("Synthetic E0 scheduler substrate measurements.");
    println!();
    println!("Options:");
    println!("  --threads <n>       Worker threads. Default: logical CPUs");
    println!("  --tasks <n>         Driver tokens circulating in queues");
    println!("  --iterations <n>    Pop/process/push iterations per worker");
    println!("  --work-us <list>    Comma-separated per-iteration spin time");
    println!("  --state-lines <n>   Per-task cache lines touched each iteration");
    println!("  --levels <n>        Atomic level-head count for hotspot test");
    println!("  --capacity <n>      Bounded MPMC capacity, power of two");
}

#[repr(align(64))]
struct TaskLine {
    value: UnsafeCell<u64>,
}

unsafe impl Sync for TaskLine {}

struct TaskState {
    lines: Vec<TaskLine>,
    lines_per_task: usize,
}

impl TaskState {
    fn new(tasks: usize, lines_per_task: usize) -> Self {
        let line_count = tasks * lines_per_task.max(1);
        let lines = (0..line_count)
            .map(|idx| TaskLine {
                value: UnsafeCell::new(idx as u64),
            })
            .collect();
        Self {
            lines,
            lines_per_task: lines_per_task.max(1),
        }
    }

    fn touch(&self, task_id: usize) {
        let start = task_id * self.lines_per_task;
        let mut acc = task_id as u64;
        for line in &self.lines[start..start + self.lines_per_task] {
            unsafe {
                let ptr = line.value.get();
                let next = (*ptr).wrapping_add(1).wrapping_add(acc);
                *ptr = next;
                acc ^= next;
            }
        }
        black_box(acc);
    }
}

struct BenchResult {
    mode: &'static str,
    threads: usize,
    tasks: usize,
    iterations_per_thread: usize,
    work_us: u64,
    ops: usize,
    wall: Duration,
    lock_wait_ns: u128,
}

impl BenchResult {
    fn print_csv_header() {
        println!(
            "mode,threads,tasks,iterations_per_thread,work_us,ops,wall_ms,ops_per_sec,lock_wait_ms,lock_wait_ns_per_op"
        );
    }

    fn print_csv(&self) {
        let wall_secs = self.wall.as_secs_f64();
        let ops_per_sec = if wall_secs > 0.0 {
            self.ops as f64 / wall_secs
        } else {
            0.0
        };
        let lock_wait_per_op = if self.ops > 0 {
            self.lock_wait_ns as f64 / self.ops as f64
        } else {
            0.0
        };
        println!(
            "{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3}",
            self.mode,
            self.threads,
            self.tasks,
            self.iterations_per_thread,
            self.work_us,
            self.ops,
            self.wall.as_secs_f64() * 1000.0,
            ops_per_sec,
            self.lock_wait_ns as f64 / 1_000_000.0,
            lock_wait_per_op
        );
    }
}

fn busy_work(work_us: u64) {
    if work_us == 0 {
        return;
    }
    let deadline = Instant::now() + Duration::from_micros(work_us);
    let mut v = 0u64;
    while Instant::now() < deadline {
        v = v.wrapping_mul(6364136223846793005).wrapping_add(1);
        black_box(v);
        spin_loop();
    }
}

struct QueueSlot {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<usize>>,
}

unsafe impl Sync for QueueSlot {}

struct BoundedMpmcQueue {
    buffer: Box<[QueueSlot]>,
    mask: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl BoundedMpmcQueue {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let buffer = (0..capacity)
            .map(|idx| QueueSlot {
                sequence: AtomicUsize::new(idx),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            buffer,
            mask: capacity - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    fn push(&self, value: usize) -> Result<(), usize> {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe {
                            (*slot.value.get()).write(value);
                        }
                        slot.sequence
                            .store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(next) => pos = next,
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                pos = self.tail.load(Ordering::Relaxed);
                spin_loop();
            }
        }
    }

    fn pop(&self) -> Option<usize> {
        let capacity = self.mask + 1;
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.buffer[pos & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos.wrapping_add(1) as isize;
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let value = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.sequence
                            .store(pos.wrapping_add(capacity), Ordering::Release);
                        return Some(value);
                    }
                    Err(next) => pos = next,
                }
            } else if diff < 0 {
                return None;
            } else {
                pos = self.head.load(Ordering::Relaxed);
                spin_loop();
            }
        }
    }
}

fn run_mutex_central(cfg: &Config, work_us: u64) -> BenchResult {
    let queue = Arc::new(Mutex::new((0..cfg.tasks).collect::<VecDeque<_>>()));
    let state = Arc::new(TaskState::new(cfg.tasks, cfg.state_lines));
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let lock_wait_ns = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for _ in 0..cfg.threads {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        let lock_wait_ns = Arc::clone(&lock_wait_ns);
        let iterations = cfg.iterations;
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..iterations {
                let before = Instant::now();
                let task = {
                    let mut guard = queue.lock().expect("mutex central queue lock");
                    lock_wait_ns.fetch_add(
                        before.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                    guard.pop_front().expect("queue should not run dry")
                };
                state.touch(task);
                busy_work(work_us);
                let before = Instant::now();
                let mut guard = queue.lock().expect("mutex central queue lock");
                lock_wait_ns.fetch_add(
                    before.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                guard.push_back(task);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "mutex_central",
        threads: cfg.threads,
        tasks: cfg.tasks,
        iterations_per_thread: cfg.iterations,
        work_us,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: lock_wait_ns.load(Ordering::Relaxed) as u128,
    }
}

fn run_atomic_central(cfg: &Config, work_us: u64) -> BenchResult {
    let queue = Arc::new(BoundedMpmcQueue::new(cfg.capacity));
    for task in 0..cfg.tasks {
        queue.push(task).expect("prefill queue");
    }
    let state = Arc::new(TaskState::new(cfg.tasks, cfg.state_lines));
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for _ in 0..cfg.threads {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..iterations {
                let task = loop {
                    if let Some(task) = queue.pop() {
                        break task;
                    }
                    spin_loop();
                };
                state.touch(task);
                busy_work(work_us);
                let mut value = task;
                loop {
                    match queue.push(value) {
                        Ok(()) => break,
                        Err(v) => {
                            value = v;
                            spin_loop();
                        }
                    }
                }
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "atomic_central",
        threads: cfg.threads,
        tasks: cfg.tasks,
        iterations_per_thread: cfg.iterations,
        work_us,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

fn run_per_worker_local(cfg: &Config, work_us: u64) -> BenchResult {
    let state = Arc::new(TaskState::new(cfg.tasks, cfg.state_lines));
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let tasks_per_worker = cfg.tasks.div_ceil(cfg.threads);
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for worker in 0..cfg.threads {
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        let tasks = cfg.tasks;
        let begin = worker * tasks_per_worker;
        let end = ((worker + 1) * tasks_per_worker).min(cfg.tasks);
        handles.push(thread::spawn(move || {
            let mut local = (begin..end).collect::<VecDeque<_>>();
            if local.is_empty() {
                local.push_back(worker % tasks);
            }
            barrier.wait();
            for _ in 0..iterations {
                let task = local.pop_front().expect("local queue should not run dry");
                state.touch(task);
                busy_work(work_us);
                local.push_back(task);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "per_worker_local",
        threads: cfg.threads,
        tasks: cfg.tasks,
        iterations_per_thread: cfg.iterations,
        work_us,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

fn run_affinity_slot(cfg: &Config, work_us: u64) -> BenchResult {
    let queue = Arc::new(BoundedMpmcQueue::new(cfg.capacity));
    for task in 0..cfg.tasks {
        queue.push(task).expect("prefill queue");
    }
    let state = Arc::new(TaskState::new(cfg.tasks, cfg.state_lines));
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for _ in 0..cfg.threads {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        handles.push(thread::spawn(move || {
            let mut slot = None;
            barrier.wait();
            for _ in 0..iterations {
                let task = match slot.take() {
                    Some(task) => task,
                    None => loop {
                        if let Some(task) = queue.pop() {
                            break task;
                        }
                        spin_loop();
                    },
                };
                state.touch(task);
                busy_work(work_us);
                slot = Some(task);
            }
            if let Some(task) = slot.take() {
                let mut value = task;
                loop {
                    match queue.push(value) {
                        Ok(()) => break,
                        Err(v) => {
                            value = v;
                            spin_loop();
                        }
                    }
                }
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "affinity_slot_upper_bound",
        threads: cfg.threads,
        tasks: cfg.tasks,
        iterations_per_thread: cfg.iterations,
        work_us,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

fn run_atomic_hotspot_packed(cfg: &Config) -> BenchResult {
    let levels = Arc::new(
        (0..cfg.levels)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>(),
    );
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for worker in 0..cfg.threads {
        let levels = Arc::clone(&levels);
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        handles.push(thread::spawn(move || {
            barrier.wait();
            for idx in 0..iterations {
                let level = (idx + worker) % levels.len();
                levels[level].fetch_add(1, Ordering::AcqRel);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "atomic_level_heads_packed",
        threads: cfg.threads,
        tasks: cfg.levels,
        iterations_per_thread: cfg.iterations,
        work_us: 0,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

#[repr(align(64))]
struct PaddedAtomic(AtomicUsize);

fn run_atomic_hotspot_padded(cfg: &Config) -> BenchResult {
    let levels = Arc::new(
        (0..cfg.levels)
            .map(|_| PaddedAtomic(AtomicUsize::new(0)))
            .collect::<Vec<_>>(),
    );
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for worker in 0..cfg.threads {
        let levels = Arc::clone(&levels);
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        handles.push(thread::spawn(move || {
            barrier.wait();
            for idx in 0..iterations {
                let level = (idx + worker) % levels.len();
                levels[level].0.fetch_add(1, Ordering::AcqRel);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "atomic_level_heads_padded",
        threads: cfg.threads,
        tasks: cfg.levels,
        iterations_per_thread: cfg.iterations,
        work_us: 0,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

fn run_atomic_hotspot_per_worker(cfg: &Config) -> BenchResult {
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for worker in 0..cfg.threads {
        let barrier = Arc::clone(&barrier);
        let iterations = cfg.iterations;
        let levels = cfg.levels;
        handles.push(thread::spawn(move || {
            let mut local = vec![0usize; levels];
            barrier.wait();
            for idx in 0..iterations {
                let level = (idx + worker) % local.len();
                local[level] = local[level].wrapping_add(1);
                black_box(local[level]);
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker join");
    }
    BenchResult {
        mode: "atomic_level_heads_per_worker",
        threads: cfg.threads,
        tasks: cfg.levels,
        iterations_per_thread: cfg.iterations,
        work_us: 0,
        ops: cfg.threads * cfg.iterations,
        wall: start.elapsed(),
        lock_wait_ns: 0,
    }
}

fn main() {
    let cfg = Config::parse().unwrap_or_else(|err| {
        eprintln!("error: {err}");
        eprintln!();
        print_help();
        std::process::exit(2);
    });

    BenchResult::print_csv_header();
    run_atomic_hotspot_packed(&cfg).print_csv();
    run_atomic_hotspot_padded(&cfg).print_csv();
    run_atomic_hotspot_per_worker(&cfg).print_csv();
    for &work_us in &cfg.work_us {
        run_mutex_central(&cfg, work_us).print_csv();
        run_atomic_central(&cfg, work_us).print_csv();
        run_per_worker_local(&cfg, work_us).print_csv();
        run_affinity_slot(&cfg, work_us).print_csv();
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedMpmcQueue;

    #[test]
    fn bounded_queue_preserves_single_thread_fifo_order() {
        let queue = BoundedMpmcQueue::new(4);

        assert!(queue.push(10).is_ok());
        assert!(queue.push(20).is_ok());

        assert_eq!(queue.pop(), Some(10));
        assert_eq!(queue.pop(), Some(20));
        assert_eq!(queue.pop(), None);
    }
}
