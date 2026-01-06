# Translate threads.c - Threading Utilities

## Overview
Translate thread management utilities for background tasks and parallel operations.

## Source Reference
- `sqlite3/src/threads.c` - ~200 lines

## Design Fidelity
- SQLite’s "Why C?" rationale (https://sqlite.org/whyc.html) is important context; we intentionally diverge by using Rust.
- Preserve SQLite’s observable behavior and architecture; favor mechanical translations over refactors.
- Keep control flow and error handling aligned to upstream unless explicitly agreed.

## Key Data Structures

### Thread Handle
```rust
/// SQLite thread handle
pub struct SqliteThread {
    /// Thread handle
    handle: Option<std::thread::JoinHandle<*mut ()>>,
    /// Thread result
    result: *mut (),
    /// Thread ID for debugging
    id: std::thread::ThreadId,
}

/// Thread task function signature
pub type ThreadTask = Box<dyn FnOnce(*mut ()) -> *mut () + Send + 'static>;
```

### Thread Pool
```rust
/// Thread pool for background operations
pub struct ThreadPool {
    /// Worker threads
    workers: Vec<Worker>,
    /// Task sender
    sender: crossbeam_channel::Sender<Task>,
    /// Number of threads
    size: usize,
}

struct Worker {
    id: usize,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Task {
    func: ThreadTask,
    arg: *mut (),
    result: Arc<Mutex<Option<*mut ()>>>,
    done: Arc<AtomicBool>,
}

// Safety: we control the lifetime of the raw pointer
unsafe impl Send for Task {}
```

## Thread Creation

```rust
impl SqliteThread {
    /// Create and start a new thread
    pub fn create(
        task: impl FnOnce(*mut ()) -> *mut () + Send + 'static,
        arg: *mut ()
    ) -> Result<Self> {
        let task = Box::new(task);

        let handle = std::thread::Builder::new()
            .name("sqlite-worker".to_string())
            .spawn(move || {
                task(arg)
            })
            .map_err(|e| Error::with_message(
                ErrorCode::Error,
                format!("failed to create thread: {}", e)
            ))?;

        let id = handle.thread().id();

        Ok(Self {
            handle: Some(handle),
            result: std::ptr::null_mut(),
            id,
        })
    }

    /// Wait for thread to complete and get result
    pub fn join(&mut self) -> Result<*mut ()> {
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(result) => {
                    self.result = result;
                    Ok(result)
                }
                Err(_) => Err(Error::with_message(
                    ErrorCode::Error,
                    "thread panicked"
                )),
            }
        } else {
            Ok(self.result)
        }
    }

    /// Check if thread is still running
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }
}
```

## Thread Pool Implementation

```rust
impl ThreadPool {
    /// Create a new thread pool
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<Task>();

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            let receiver = receiver.clone();

            let thread = std::thread::Builder::new()
                .name(format!("sqlite-pool-{}", id))
                .spawn(move || {
                    while let Ok(task) = receiver.recv() {
                        let result = (task.func)(task.arg);
                        *task.result.lock().unwrap() = Some(result);
                        task.done.store(true, Ordering::SeqCst);
                    }
                })
                .ok();

            workers.push(Worker { id, thread });
        }

        Self { workers, sender, size }
    }

    /// Submit a task to the pool
    pub fn submit(
        &self,
        func: impl FnOnce(*mut ()) -> *mut () + Send + 'static,
        arg: *mut ()
    ) -> TaskHandle {
        let result = Arc::new(Mutex::new(None));
        let done = Arc::new(AtomicBool::new(false));

        let task = Task {
            func: Box::new(func),
            arg,
            result: result.clone(),
            done: done.clone(),
        };

        self.sender.send(task).expect("thread pool shut down");

        TaskHandle { result, done }
    }

    /// Shutdown the pool
    pub fn shutdown(self) {
        drop(self.sender);  // Close channel

        for worker in self.workers {
            if let Some(thread) = worker.thread {
                let _ = thread.join();
            }
        }
    }
}

pub struct TaskHandle {
    result: Arc<Mutex<Option<*mut ()>>>,
    done: Arc<AtomicBool>,
}

impl TaskHandle {
    /// Wait for task completion
    pub fn wait(&self) -> *mut () {
        while !self.done.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        self.result.lock().unwrap().unwrap_or(std::ptr::null_mut())
    }

    /// Check if task is complete
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }
}
```

## Thread-Local Storage

```rust
/// Thread-local storage key
pub struct TlsKey<T: 'static> {
    key: std::thread::LocalKey<RefCell<Option<T>>>,
}

impl<T: 'static> TlsKey<T> {
    pub const fn new() -> Self {
        thread_local! {
            static VALUE: RefCell<Option<T>> = RefCell::new(None);
        }
        Self { key: VALUE }
    }

    pub fn get<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&T>) -> R
    {
        self.key.with(|cell| {
            f(cell.borrow().as_ref())
        })
    }

    pub fn set(&self, value: T) {
        self.key.with(|cell| {
            *cell.borrow_mut() = Some(value);
        });
    }

    pub fn take(&self) -> Option<T> {
        self.key.with(|cell| {
            cell.borrow_mut().take()
        })
    }
}
```

## Threading Configuration

```rust
/// Threading mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThreadingMode {
    /// Single-threaded - no mutexes
    SingleThread,
    /// Multi-threaded - serialized access
    MultiThread,
    /// Serialized - thread-safe
    Serialized,
}

static THREADING_MODE: AtomicU8 = AtomicU8::new(ThreadingMode::Serialized as u8);

pub fn set_threading_mode(mode: ThreadingMode) -> Result<()> {
    // Can only be set before initialization
    THREADING_MODE.store(mode as u8, Ordering::SeqCst);
    Ok(())
}

pub fn get_threading_mode() -> ThreadingMode {
    match THREADING_MODE.load(Ordering::SeqCst) {
        0 => ThreadingMode::SingleThread,
        1 => ThreadingMode::MultiThread,
        _ => ThreadingMode::Serialized,
    }
}

pub fn is_threadsafe() -> bool {
    get_threading_mode() != ThreadingMode::SingleThread
}
```

## C-Compatible Interface

```rust
/// Create thread (C-compatible signature)
pub extern "C" fn sqlite3_thread_create(
    task: extern "C" fn(*mut ()) -> *mut (),
    arg: *mut ()
) -> *mut SqliteThread {
    let task_wrapper = move |arg: *mut ()| -> *mut () {
        task(arg)
    };

    match SqliteThread::create(task_wrapper, arg) {
        Ok(thread) => Box::into_raw(Box::new(thread)),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Join thread (C-compatible signature)
pub extern "C" fn sqlite3_thread_join(
    thread: *mut SqliteThread,
    result: *mut *mut ()
) -> i32 {
    if thread.is_null() {
        return ErrorCode::Error as i32;
    }

    let thread = unsafe { &mut *thread };
    match thread.join() {
        Ok(r) => {
            if !result.is_null() {
                unsafe { *result = r };
            }
            ErrorCode::Ok as i32
        }
        Err(_) => ErrorCode::Error as i32,
    }
}

/// Free thread handle
pub extern "C" fn sqlite3_thread_free(thread: *mut SqliteThread) {
    if !thread.is_null() {
        unsafe { drop(Box::from_raw(thread)) };
    }
}
```

## Parallel Sort Support

```rust
/// Parallel sorting context
pub struct ParallelSort<T: Send + Ord> {
    data: Vec<T>,
    threshold: usize,
}

impl<T: Send + Ord + Clone> ParallelSort<T> {
    pub fn new(data: Vec<T>) -> Self {
        Self {
            data,
            threshold: 1000,  // Use parallel sort above this size
        }
    }

    pub fn sort(mut self) -> Vec<T> {
        if self.data.len() < self.threshold {
            self.data.sort();
        } else {
            // Use rayon for parallel sort if available
            #[cfg(feature = "parallel")]
            {
                use rayon::prelude::*;
                self.data.par_sort();
            }
            #[cfg(not(feature = "parallel"))]
            {
                self.data.sort();
            }
        }
        self.data
    }
}
```

## Thread Safety Assertions

```rust
/// Debug assertion that we're on the expected thread
#[cfg(debug_assertions)]
pub struct ThreadAssertion {
    thread_id: std::thread::ThreadId,
}

#[cfg(debug_assertions)]
impl ThreadAssertion {
    pub fn new() -> Self {
        Self {
            thread_id: std::thread::current().id(),
        }
    }

    pub fn assert_same_thread(&self) {
        assert_eq!(
            self.thread_id,
            std::thread::current().id(),
            "called from wrong thread"
        );
    }
}

#[cfg(not(debug_assertions))]
pub struct ThreadAssertion;

#[cfg(not(debug_assertions))]
impl ThreadAssertion {
    pub fn new() -> Self { Self }
    pub fn assert_same_thread(&self) {}
}
```

## Acceptance Criteria
- [ ] SqliteThread creation and joining
- [ ] Thread pool implementation
- [ ] Task submission and waiting
- [ ] Thread-local storage
- [ ] Threading mode configuration
- [ ] Single-threaded mode support
- [ ] Multi-threaded serialized mode
- [ ] C-compatible thread API
- [ ] Thread safety assertions (debug)
- [ ] Parallel sort support
- [ ] Proper cleanup on thread exit
- [ ] Error handling for thread failures
