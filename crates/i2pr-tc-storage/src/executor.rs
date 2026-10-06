//! Bounded offload seam for blocking storage work.
//!
//! The storage crate stays free of an async runtime so the same synchronous
//! surface works for the local RPC adapter and for peer tasks. Callers that
//! already own an async runtime inject an executor here, and
//! [`crate::BlockingStoragePool`] is the bounded implementation used when no
//! runtime is available.
use crate::StorageError;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    task::{Context, Poll, Waker},
    thread,
};

/// One unit of blocking work handed to an executor.
///
/// Public so a caller can implement its own executor against
/// [`StorageExecutor`], for example a pool with its own scheduling policy.
pub type BlockingJob = Box<dyn FnOnce() + Send + 'static>;

/// Maximum blocking worker threads a single pool may start.
pub const MAX_STORAGE_WORKERS: usize = 256;
/// Maximum queued jobs a single pool may accept.
pub const MAX_STORAGE_QUEUE: usize = 4096;

/// Blocking work handed to an executor by the async persistence API.
///
/// Implementations must bound queued work. `Ok(())` promises the job will run
/// to completion; `Err` promises it was never started and that the caller must
/// treat the operation as not persisted.
pub trait StorageExecutor: Send + Sync + 'static {
    fn spawn_blocking(&self, job: BlockingJob) -> Result<(), StorageError>;
}

/// Fixed-size blocking worker pool with a bounded submission queue.
///
/// The pool is the bounded backpressure authority for storage offload: with
/// `workers` threads and a `queue_capacity` slots, a submission that finds the
/// queue full fails immediately with [`StorageError::Backpressure`] instead of
/// queueing without limit or blocking the caller.
pub struct BlockingStoragePool {
    sender: SyncSender<BlockingJob>,
    workers: usize,
    queue_capacity: usize,
}

impl BlockingStoragePool {
    /// Start `workers` threads behind a queue of `queue_capacity` slots.
    pub fn new(workers: usize, queue_capacity: usize) -> Result<Self, StorageError> {
        if workers == 0 || workers > MAX_STORAGE_WORKERS {
            return Err(StorageError::InvalidInput);
        }
        if queue_capacity == 0 || queue_capacity > MAX_STORAGE_QUEUE {
            return Err(StorageError::InvalidInput);
        }
        let (sender, receiver) = sync_channel::<BlockingJob>(queue_capacity);
        // `Receiver` is not `Clone`, so workers share one receiver behind a
        // mutex. Each worker holds it only for the `recv` call itself, so the
        // queue still hands each job to exactly one worker.
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..workers {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("i2pr-tc-storage-{index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let guard = match receiver.lock() {
                                Ok(guard) => guard,
                                Err(_) => return,
                            };
                            guard.recv()
                        };
                        match job {
                            Ok(job) => job(),
                            Err(_) => return,
                        }
                    }
                })
                .map_err(StorageError::Io)?;
        }
        Ok(Self {
            sender,
            workers,
            queue_capacity,
        })
    }

    pub fn worker_count(&self) -> usize {
        self.workers
    }

    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }
}

impl Default for BlockingStoragePool {
    fn default() -> Self {
        // Four blocking workers and sixteen queued jobs keeps multi-torrent
        // persistence concurrent without letting submissions grow unbounded.
        Self::new(4, 16).expect("bounded storage pool defaults are valid")
    }
}

impl StorageExecutor for BlockingStoragePool {
    fn spawn_blocking(&self, job: BlockingJob) -> Result<(), StorageError> {
        match self.sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(StorageError::Backpressure),
            Err(TrySendError::Disconnected(_)) => Err(StorageError::Concurrency),
        }
    }
}

struct SlotState<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// One-shot slot shared by an executor job and the future awaiting it.
///
/// The first `complete` wins and wakes the awaiting future; later completions
/// are ignored, so a job can publish a result even if nobody is waiting.
pub(crate) struct ResultSlot<T> {
    state: Mutex<SlotState<T>>,
}

impl<T> ResultSlot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState {
                value: None,
                waker: None,
            }),
        })
    }

    fn state(&self) -> MutexGuard<'_, SlotState<T>> {
        // A panicking job must not wedge the awaiting peer task; the slot keeps
        // serving whatever value was published before the panic.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn complete(&self, value: T) {
        let waker = {
            let mut state = self.state();
            if state.value.is_some() {
                return;
            }
            state.value = Some(value);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn poll_take(&self, context: &mut Context<'_>) -> Poll<T> {
        let mut state = self.state();
        match state.value.take() {
            Some(value) => Poll::Ready(value),
            None => {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<T> Future for ResultSlot<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        self.poll_take(context)
    }
}

/// Awaitable handle for a slot that a job completes.
///
/// The slot itself is shared with the worker thread, so awaiting goes through
/// this handle instead of borrowing the slot in place.
pub(crate) struct SlotFuture<T> {
    slot: Arc<ResultSlot<T>>,
}

impl<T> Future for SlotFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        self.slot.poll_take(context)
    }
}

impl<T> ResultSlot<T> {
    pub(crate) fn waiter(self: &Arc<Self>) -> SlotFuture<T> {
        SlotFuture {
            slot: Arc::clone(self),
        }
    }
}
