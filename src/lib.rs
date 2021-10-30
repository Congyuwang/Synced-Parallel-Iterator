#![cfg_attr(feature = "bench", feature(test))]
//!
//! # par_iter_sync: Parallel Iterator With Sequential Output
//!
//! Crate like `rayon` do not offer synchronization mechanism.
//! This crate provides easy mixture of parallelism and synchronization.
//! Execute tasks in concurrency with synchronization at any steps.
//!
//! Consider the case where multiple threads share a cache which can be read
//! only after prior tasks have written to it (e.g., reads of task 4 depends
//! on writes of task 1-4).
//!
//! Using `IntoParallelIteratorSync` trait
//!```
//! // in concurrency: task1 write | task2 write | task3 write | task4 write
//! //                      \_____________\_____________\_____________\
//! //             task4 read depends on task 1-4 write  \___________
//! //                                                               \
//! // in concurrency:              | task2 read  | task3 read  | task4 read
//!
//! use par_iter_sync::IntoParallelIteratorSync;
//! use std::sync::{Arc, Mutex};
//! use std::collections::HashSet;
//!
//! // there are 100 tasks
//! let tasks = 0..100;
//!
//! // an in-memory cache for integers
//! let cache: Arc<Mutex<HashSet<i32>>> = Arc::new(Mutex::new(HashSet::new()));
//! let cache_clone = cache.clone();
//!
//! // iterate through tasks
//! tasks.into_par_iter_sync(move |task_number| {
//!
//!     // writes cache (write the integer in cache), in parallel
//!     cache.lock().unwrap().insert(task_number);
//!     // return the task number to the next iterator
//!     Ok(task_number)
//!
//! }).into_par_iter_sync(move |task_number| { // <- synced to sequential order
//!
//!     // reads
//!     assert!(cache_clone.lock().unwrap().contains(&task_number));
//!     Ok(())
//! // append a for each to actually run the whole chain
//! }).for_each(|_| ());
//!```
//!
//! ## Usage Caveat
//! This crate is designed to clone all resources captured by the closure
//! for each thread. To prevent unintended RAM usage, you may wrap
//! large data structure using `Arc`.
//!
//! ## Sequential Consistency
//! The output order is guaranteed to be the same as the upstream iterator,
//! but the execution order is not sequential.
//!
//! ## Examples
//!
//! ### Mix Syncing and Parallelism By Chaining
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//!
//! (0..100).into_par_iter_sync(|i| {
//!     Ok(i)                   // <~ async execution
//! }).into_par_iter_sync(|i| { // <- sync order
//!     Ok(i)                   // <~async execution
//! }).into_par_iter_sync(|i| { // <- sync order
//!     Ok(i)                   // <~async execution
//! }).for_each(|x| ());        // <- sync order
//! ```
//!
//! ### Use `std::iter::IntoIterator` interface
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//!
//! let mut count = 0;
//!
//! // for loop
//! for i in (0..100).into_par_iter_sync(|i| Ok(i)) {
//!     assert_eq!(i, count);
//!     count += 1;
//! }
//!
//! // sum
//! let sum: i32 = (1..=100).into_par_iter_sync(|i| Ok(i)).sum();
//!
//! // skip, take and collect
//! let results: Vec<i32> = (0..10)
//!     .into_par_iter_sync(|i| Ok(i))
//!     .skip(1)
//!     .take(5)
//!     .collect();
//!
//! assert_eq!(sum, 5050);
//! assert_eq!(results, vec![1, 2, 3, 4, 5])
//! ```
//!
//! ### Bridge To Rayon
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//! use rayon::prelude::*;
//!
//! // sum with rayon
//! let sum: i32 = (1..=100)
//!     .into_par_iter_sync(|i| Ok(i))
//!     .par_bridge()    // <- switch to rayon
//!     .into_par_iter()
//!     .sum();
//!
//! assert_eq!(sum, 5050);
//! ```
//!
//! ### Closure Captures Variables
//! Variables captured are cloned to each thread automatically.
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//! use std::sync::Arc;
//!
//! // use `Arc` to save RAM
//! let resource_captured = Arc::new(vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3]);
//! let len = resource_captured.len();
//!
//! let result_iter = (0..len).into_par_iter_sync(move |i| {
//!     // `resource_captured` is moved into the closure
//!     // and cloned to worker threads.
//!     let read_from_resource = resource_captured.get(i).unwrap();
//!     Ok(*read_from_resource)
//! });
//!
//! // the result is produced in sequential order
//! let collected: Vec<i32> = result_iter.collect();
//! assert_eq!(collected, vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3])
//! ```
//!
//! ### Fast Fail During Exception
//! The iterator stops once the inner function returns an `Err`.
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//! use std::sync::Arc;
//! use log::warn;
//!
//! /// this function returns `Err` when it reads 1000
//! fn error_at_1000(n: i32) -> Result<i32, ()> {
//!     if n == 1000 {
//!         // you may log this error
//!         warn!("Some Error Occurs");
//!         Err(())
//!     } else {
//!         Ok(n)
//!     }
//! }
//!
//! let results: Vec<i32> = (0..10000).into_par_iter_sync(move |a| {
//!     Ok(a)
//! }).into_par_iter_sync(move |a| {
//!     // error at 1000
//!     error_at_1000(a)
//! }).into_par_iter_sync(move |a| {
//!     Ok(a)
//! }).collect();
//!
//! let expected: Vec<i32> = (0..1000).collect();
//! assert_eq!(results, expected)
//! ```
//!
//! #### You may choose to skip error
//! If you do not want to stop on `Err`, this is a workaround.
//! ```
//! use par_iter_sync::IntoParallelIteratorSync;
//! use std::sync::Arc;
//!
//! let results: Vec<Result<i32, ()>> = (0..5).into_par_iter_sync(move |n| {
//!     // error at 3, but skip
//!     if n == 3 {
//!         Ok(Err(()))
//!     } else {
//!         Ok(Ok(n))
//!     }
//! }).collect();
//!
//! assert_eq!(results, vec![Ok(0), Ok(1), Ok(2), Err(()), Ok(4)])
//! ```
//! ## Overhead Benchmark
//! Platform: Macbook Air (2015 Late) 8 GB RAM, Intel Core i5, 1.6GHZ (2 Core).
//!
//! ### Result
//! One million (1,000,000) empty iteration for each run.
//! ```text
//! test iter_async::test_par_iter_async::bench_into_par_iter_async
//!     ... bench: 110,277,577 ns/iter (+/- 28,510,054)
//!
//! test test_par_iter::bench_into_par_iter_sync
//!     ... bench: 121,063,787 ns/iter (+/- 103,787,056)
//! ```
//!
//! Result:
//! - Async iterator overhead `110 ns (+/-  28 ns)`.
//! - Sync iterator overhead  `121 ns (+/- 103 ns)`.
//!
//! ## Implementation Note
//!
//! ### Output Buffering
//! - Each worker use a synced single-producer mpsc channel to buffer outputs.
//!   So, when a thread is waiting for its turn to get polled, it does not
//!   get blocked. The channel size is hard-coded to 100 for each thread.
//! - The number of threads equals to the number of logical cores.
//!
//! ### Synchronization Mechanism
//! - When each thread fetch a task, it registers its thread ID (`thread_number`)
//!   and the task ID (`task_number`) into a mpsc channel.
//! - When `next()` is called, the consumer fetch from the task registry
//!   (`task_order`) the next thread ID and task ID.
//! - If `next()` detect that some thread has not produced result due to exception,
//!   it calls `kill()`, which stop threads from fetching new tasks,
//!   flush remaining tasks, and joining the worker threads.
//!
//! ### Error handling and Dropping
//! - When any exception occurs, stop producers from fetching new task.
//! - Before dropping the structure, stop all producers from fetching tasks,
//!   flush all remaining tasks, and join all threads..
//!
mod iter_async;

use crossbeam::channel::{bounded, Receiver};
use crossbeam::sync::{Parker, Unparker};
use crossbeam::utils::Backoff;
pub use iter_async::*;
use num_cpus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

const MAX_SIZE_FOR_THREAD: usize = 100;

pub trait IntoParallelIteratorSync<R, T, TL, F>
where
    F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
    T: Send,
    TL: Send + IntoIterator<Item = T>,
    <TL as IntoIterator>::IntoIter: Send + 'static,
    R: Send,
{
    ///
    /// # Usage
    ///
    /// This method executes `func` in parallel.
    ///
    /// The `func` is a closure that takes the returned elements
    /// from the upstream iterator as argument and returns
    /// some `Result(R, ())`.
    ///
    /// This iterator would return type `R` when it gets `Ok(R)`
    /// and stops when it gets an `Err(())`.
    ///
    /// ## Example
    ///
    /// ```
    /// use par_iter_sync::IntoParallelIteratorSync;
    ///
    /// let mut count = 0;
    ///
    /// // for loop
    /// for i in (0..100).into_par_iter_sync(|i| Ok(i)) {
    ///     assert_eq!(i, count);
    ///     count += 1;
    /// }
    ///
    /// // sum
    /// let sum: i32 = (1..=100).into_par_iter_sync(|i| Ok(i)).sum();
    ///
    /// // take and collect
    /// let results: Vec<i32> = (0..10).into_par_iter_sync(|i| Ok(i)).take(5).collect();
    ///
    /// assert_eq!(sum, 5050);
    /// assert_eq!(results, vec![0, 1, 2, 3, 4])
    /// ```
    ///
    /// If the result is not polled using `next()`,
    /// the parallel execution will stop and wait.
    ///
    /// ## Sequential Consistency
    /// The output order is guaranteed to be the same as the provided iterator.
    ///
    /// See [crate] module-level doc.
    ///
    fn into_par_iter_sync(self, func: F) -> ParIterSync<R>;
}

impl<R, T, TL, F> IntoParallelIteratorSync<R, T, TL, F> for TL
where
    F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
    T: Send,
    TL: Send + IntoIterator<Item = T>,
    <TL as IntoIterator>::IntoIter: Send + 'static,
    R: Send + 'static,
{
    fn into_par_iter_sync(self, func: F) -> ParIterSync<R> {
        ParIterSync::new(self, func)
    }
}

///
/// A lookup table to register and look up corresponding thread id for a task.
///
/// `-1` represents the task ID is not yet registered
/// or the task does not exist.
///
struct TaskRegistry {
    // vector of thread ID
    inner: Arc<Vec<AtomicIsize>>,
    parkers: Vec<Parker>,
}

impl Deref for TaskRegistry {
    type Target = Vec<AtomicIsize>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

/// Write client for Task registry
struct TaskRegistryWrite {
    inner: Arc<Vec<AtomicIsize>>,
    unparkers: Vec<Unparker>
}

impl Deref for TaskRegistryWrite {
    type Target = Vec<AtomicIsize>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl Drop for TaskRegistryWrite {
    fn drop(&mut self) {
        for unparker in &self.unparkers {
            unparker.unpark();
        }
    }
}

impl TaskRegistry {

    ///
    /// Initialize the registry with `-1` to represent an empty registry
    ///
    /// The `size` must be (just) big enough to ensure that no key collision would
    /// possibly occur.
    ///
    fn new(size: usize) -> TaskRegistry {
        TaskRegistry {
            inner: Arc::new((0..size).map(|_| AtomicIsize::new(-1)).collect()),
            parkers: (0..size).map(|_| Parker::new()).collect()
        }
    }

    ///
    /// Look up a thread_number of a task and set that slot to `-1`.
    ///
    /// This function blocks to wait for a task to be registered,
    /// unless all worker threads have stopped so that no more new
    /// task can possibly be registered.
    ///
    /// It should block very rarely since task dispatcher is not blocking,
    /// and is registered immediately after fetching in `get_task`.
    ///
    /// returns `None` only when all worker threads have stopped
    ///
    #[inline(always)]
    pub(crate) fn lookup(&self, task_id: usize) -> Option<isize> {
        let registry_len = self.len();
        let pos = TaskRegistry::id_to_key(task_id, registry_len);
        let backoff = Backoff::new();
        loop {
            // check if worker threads are still active
            if !self.is_disconnected() {
                let thread_num = self[pos].swap(-1, Ordering::SeqCst);
                // if `-1` is read, would continue in the loop
                if thread_num >= 0 {
                    return Some(thread_num);
                } else {
                    // snooze
                    if backoff.is_completed() {
                        // park but no more than 500 millis
                        self.parkers[pos].park_timeout(Duration::from_millis(500));
                    } else {
                        backoff.snooze();
                    }
                }
            // if worker threads are no more active, might return `None`
            } else {
                let thread_num = self[pos].swap(-1, Ordering::SeqCst);
                return if thread_num >= 0 {
                    Some(thread_num)
                } else {
                    None
                };
            }
        }
    }

    /// key of task ID in registry
    #[inline(always)]
    fn id_to_key(task_id: usize, registry_len: usize) -> usize {
        task_id % registry_len
    }

    fn to_write(&self) -> TaskRegistryWrite {
        TaskRegistryWrite {
            inner: self.inner.clone(),
            unparkers: self.parkers.iter().map(|p| p.unparker().clone()).collect(),
        }
    }

    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }
}

impl TaskRegistryWrite {
    ///
    /// When a worker fetches a new task, it calls `register` to tell
    /// the user thread its own thread ID.
    ///
    /// register the worker thread number of a task
    ///
    #[inline(always)]
    pub(crate) fn register(&self, task_id: usize, thread_id: isize) {
        let registry_len = self.len();
        let key = TaskRegistry::id_to_key(task_id, registry_len);
        // never overwrite
        debug_assert_eq!(self[key].load(Ordering::SeqCst), -1);
        self[key].store(thread_id, Ordering::SeqCst);
        self.unparkers[key].unpark();
    }
}

///
/// implementation of lock-free sequential parallel iterator
///
pub struct ParIterSync<R> {
    /// Result receivers, one for each worker thread
    receivers: Vec<Receiver<R>>,
    /// Receiver<thread>
    task_order: Receiver<usize>,
    /// handles to join worker threads
    worker_thread: Option<Vec<JoinHandle<()>>>,
    /// flag to stop workers from fetching new tasks
    iterator_stopper: Arc<AtomicBool>,
    /// indicate that workers have all been killed
    is_killed: bool,
}

impl<R> ParIterSync<R>
where
    R: Send + 'static,
{
    ///
    /// the worker threads are dispatched in this `new` constructor!
    ///
    pub fn new<T, TL, F>(tasks: TL, task_executor: F) -> Self
    where
        F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
        T: Send,
        TL: Send + IntoIterator<Item = T>,
        <TL as IntoIterator>::IntoIter: Send + 'static,
    {
        let cpus = num_cpus::get();
        let iterator_stopper = Arc::new(AtomicBool::new(false));

        // `(1 + MAX_SIZE_FOR_THREAD)` * cpus as there might be one more fetching after send blocking
        let task_registry: TaskRegistry = TaskRegistry::new((1 + MAX_SIZE_FOR_THREAD) * cpus);

        // this thread dispatches tasks to worker threads
        let (dispatcher, task_receiver) = bounded(MAX_SIZE_FOR_THREAD * cpus);
        let sender_thread = thread::spawn(move || {
            for (task_id, t) in tasks.into_iter().enumerate() {
                if dispatcher.send((t, task_id)).is_err() {
                    break;
                }
            }
        });

        // spawn worker threads
        let mut handles = Vec::with_capacity(cpus + 1);
        let mut output_receivers = Vec::with_capacity(cpus);
        for thread_number in 0..cpus as isize {
            let (output_sender, output_receiver) = bounded(MAX_SIZE_FOR_THREAD);
            let task_receiver = task_receiver.clone();
            let task_registry = task_registry.to_write();
            let iterator_stopper = iterator_stopper.clone();
            let task_executor = task_executor.clone();

            // workers
            let handle = thread::spawn(move || {
                loop {
                    if iterator_stopper.load(Ordering::SeqCst) {
                        break;
                    }
                    match get_task(&task, &register, thread_number) {
                        // finish
                        None => break,
                        Some(task) => match task_executor(task) {
                            Ok(blk) => {
                                sender.send(blk).unwrap();
                            }
                            Err(_) => {
                                iterator_stopper.fetch_or(true, Ordering::SeqCst);
                                break;
                            }
                        },
                    }
                }
            });
            receivers.push(receiver);
            handles.push(handle);
        }

        ParIterSync {
            receivers,
            task_order,
            worker_thread: Some(handles),
            iterator_stopper,
            is_killed: false,
        }
    }
}

impl<R> ParIterSync<R> {
    ///
    /// stop workers from fetching new tasks, and flush remaining works
    /// to prevent blocking.
    ///
    pub fn kill(&mut self) {
        if !self.is_killed {
            // stop threads from getting new tasks
            self.iterator_stopper.fetch_or(true, Ordering::SeqCst);
            // flush the remaining tasks in the channel
            loop {
                let _ = match self.task_order.recv() {
                    Ok(thread_number) => self.receivers.get(thread_number).unwrap().recv(),
                    // all workers have stopped
                    Err(_) => break,
                };
            }
            // loop break only when task_order is dropped (all workers have stopped)
            self.is_killed = true;
        }
    }
}

///
/// A helper function that locks tasks,
/// register thread_number and task_number
/// before releasing tasks lock.
///
#[inline(always)]
fn get_task<T>(
    tasks: &Receiver<(T, usize)>,
    registry: &TaskRegistryWrite,
    thread_number: isize,
) -> Option<T>
where
    T: Send,
    TL: Iterator<Item = T>,
{
    // lock task list
    let mut task = tasks.lock().unwrap();
    let next_task = task.next();
    // register task stealing
    match next_task {
        Some(task) => {
            register.send(thread_number).unwrap();
            Some(task)
        }
        None => None,
    }
}

impl<R> Iterator for ParIterSync<R> {
    type Item = R;

    ///
    /// The output API, use next to fetch result from the iterator.
    ///
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_killed {
            return None;
        }
        match self.task_order.recv() {
            Ok(thread_number) => {
                match self.receivers.get(thread_number).unwrap().recv() {
                    Ok(block) => {
                        Some(block)
                    }
                    // some worker have stopped
                    Err(_) => {
                        self.kill();
                        None
                    }
                }
            }
            // all workers have stopped
            Err(_) => None,
        }
    }
}

impl<R> ParIterSync<R> {
    ///
    /// Join worker threads. This can be only called once.
    /// Otherwise it results in panic.
    /// This is automatically called in `join()`
    ///
    fn join(&mut self) {
        for handle in self.worker_thread.take().unwrap() {
            handle.join().unwrap()
        }
    }
}

impl<R> Drop for ParIterSync<R> {
    ///
    /// Stop worker threads, join the threads.
    ///
    fn drop(&mut self) {
        self.kill();
        self.join();
    }
}

#[cfg(test)]
mod test_par_iter {
    #[cfg(feature = "bench")]
    extern crate test;
    use crate::IntoParallelIteratorSync;
    #[cfg(feature = "bench")]
    use test::Bencher;

    fn error_at_1000(test_vec: &Vec<i32>, a: i32) -> Result<i32, ()> {
        let n = test_vec.get(a as usize).unwrap().to_owned();
        if n == 1000 {
            Err(())
        } else {
            Ok(n)
        }
    }

    #[test]
    fn par_iter_test_exception() {
        for _ in 0..100 {
            let resource_captured = vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3];
            let results_expected = vec![3, 1, 4, 1];

            // if Err(()) is returned, the iterator stops early
            let results: Vec<i32> = (0..resource_captured.len())
                .into_par_iter_sync(move |a| {
                    let n = resource_captured.get(a).unwrap().to_owned();
                    if n == 5 {
                        Err(())
                    } else {
                        Ok(n)
                    }
                })
                .collect();

            assert_eq!(results, results_expected)
        }
    }

    ///
    /// The iterators can be chained.
    ///
    /// par_iter_0 -> owned by -> par_iter_1 -> owned by -> par_iter_2
    ///
    /// par_iter_1 exception at height 1000,
    ///
    /// the final output should contain 0..1000;
    ///
    #[test]
    fn par_iter_chained_exception() {
        for _ in 0..100 {
            let resource_captured: Vec<i32> = (0..10000).collect();
            let resource_captured_1 = resource_captured.clone();
            let resource_captured_2 = resource_captured.clone();
            let results_expected: Vec<i32> = (0..1000).collect();

            let results: Vec<i32> = (0..resource_captured.len())
                .into_par_iter_sync(move |a| Ok(resource_captured.get(a).unwrap().to_owned()))
                .into_par_iter_sync(move |a| error_at_1000(&resource_captured_1, a))
                .into_par_iter_sync(move |a| {
                    Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
                })
                .collect();

            assert_eq!(results, results_expected)
        }
    }

    ///
    /// par_iter_0 -> owned by -> par_iter_1 -> owned by -> par_iter_2
    ///
    /// par_iter_2 exception at height 1000,
    ///
    /// the final output should contain 0..1000;
    ///
    #[test]
    fn par_iter_chained_exception_1() {
        for _ in 0..100 {
            let resource_captured: Vec<i32> = (0..10000).collect();
            let resource_captured_1 = resource_captured.clone();
            let resource_captured_2 = resource_captured.clone();
            let results_expected: Vec<i32> = (0..1000).collect();

            let results: Vec<i32> = (0..resource_captured.len())
                .into_par_iter_sync(move |a| Ok(resource_captured.get(a).unwrap().to_owned()))
                .into_par_iter_sync(move |a| {
                    Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
                })
                .into_par_iter_sync(move |a| error_at_1000(&resource_captured_1, a))
                .collect();

            assert_eq!(results, results_expected)
        }
    }

    ///
    /// par_iter_0 -> owned by -> par_iter_1 -> owned by -> par_iter_2
    ///
    /// par_iter_0 exception at height 1000,
    ///
    /// the final output should contain 0..1000;
    ///
    #[test]
    fn par_iter_chained_exception_2() {
        for _ in 0..100 {
            let resource_captured: Vec<i32> = (0..10000).collect();
            let resource_captured_1 = resource_captured.clone();
            let resource_captured_2 = resource_captured.clone();
            let results_expected: Vec<i32> = (0..1000).collect();

            let results: Vec<i32> = (0..resource_captured.len())
                .into_par_iter_sync(move |a| error_at_1000(&resource_captured_1, a as i32))
                .into_par_iter_sync(move |a| Ok(resource_captured.get(a as usize).unwrap().to_owned()))
                .into_par_iter_sync(move |a| {
                    Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
                })
                .collect();

            assert_eq!(results, results_expected)
        }
    }

    #[test]
    fn test_break() {
        for _ in 0..100 {
            let mut count = 0;
            for i in (0..20000).into_par_iter_sync(|a| Ok(a)) {
                if i == 10000 {
                    break;
                }
                count += 1;
            }
            assert_eq!(count, 10000)
        }
    }

    #[test]
    fn test_large_iter() {
        for _ in 0..10 {
            let mut count = 0;
            for i in (0..1_000_000).into_par_iter_sync(|i| Ok(i)) {
                assert_eq!(i, count);
                count += 1;
            }
            assert_eq!(count, 1_000_000)
        }
    }

    #[cfg(feature = "bench")]
    #[bench]
    fn bench_into_par_iter_sync(b: &mut Bencher) {
        b.iter(|| {
            (0..1_000_000)
                .into_par_iter_sync(|a| Ok(a))
                .for_each(|_| {})
        });
    }
}
