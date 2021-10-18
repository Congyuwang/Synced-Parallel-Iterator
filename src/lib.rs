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
//! Using `IntoParallelIteratorSynced` trait
//!```
//! // in concurrency: task1 write | task2 write | task3 write | task4 write
//! //                      \_____________\_____________\_____________\
//! //             task4 read depends on task 1-4 write  \___________
//! //                                                               \
//! // in concurrency:              | task2 read  | task3 read  | task4 read
//!
//! use par_iter_sync::IntoParallelIteratorSynced;
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
//!
//! });
//!```
//!
//! ## Sequential Consistency
//! The output order is guaranteed to be the same as the upstream iterator,
//! but the execution order is not sequential.
//!
//! ## Examples
//!
//! ### Mix Syncing and Parallelism By Chaining
//! ```
//! use par_iter_sync::IntoParallelIteratorSynced;
//!
//! (0..100).into_par_iter_sync(|i| {
//!     Ok(i)                     // <~ async execution
//! }).into_par_iter_sync(|i| { // <- sync order
//!     Ok(i)                     // <~async execution
//! }).into_par_iter_sync(|i| { // <- sync order
//!     Ok(i)                     // <~async execution
//! });                           // <- sync order
//! ```
//!
//! ### Use `std::iter::IntoIterator` interface
//! ```
//! use par_iter_sync::IntoParallelIteratorSynced;
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
//! // take and collect
//! let results: Vec<i32> = (0..10).into_par_iter_sync(|i| Ok(i)).take(5).collect();
//!
//! assert_eq!(sum, 5050);
//! assert_eq!(results, vec![0, 1, 2, 3, 4])
//! ```
//!
//! ### Closure Captures Variables
//! Variables captured are cloned to each threads automatically.
//! ```
//! use par_iter_sync::IntoParallelIteratorSynced;
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
//! use par_iter_sync::IntoParallelIteratorSynced;
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
//! use par_iter_sync::IntoParallelIteratorSynced;
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
//!
//! ## Implementation Note
//!
//! ### Output Buffering
//! - Each worker use a synced single-producer mpsc channel to buffer outputs.
//!   So, when a thread is waiting for its turn to get polled, it does not
//!   get blocked. The channel size is hard-coded to 10 for each thread.
//! - The number of threads equals to the number of logical cores.
//!
//! ### Synchronization Mechanism
//! - When each thread fetch a task, it registers its thread ID (`thread_number`)
//!   and the task ID (`task_number`) into a mpsc channel.
//! - When `next()` is called, the consumer fetch from the task registry
//!   (`task_order`) the next thread ID and task ID.
//!   It then receives from the channel of that thread, and checks whether
//!   the current task (`current`) matches the task ID to ensure that no thread
//!   has run into exception.
//! - If `next()` detect that some thread has not produced result due to exception,
//!   it calls `kill()`, which stop threads from fetching new tasks,
//!   flush remaining tasks, and joining the worker threads.
//!
//! ### Error handling and Dropping
//! - When any exception occurs, stop producers from fetching new task.
//! - Before dropping the structure, stop all producers from fetching tasks,
//!   flush all remaining tasks, and join all threads..
//!
use std::iter::Enumerate;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use num_cpus;

const MAX_SIZE_FOR_THREAD: usize = 10;

pub trait IntoParallelIteratorSynced<R, T, TL, F>
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
    /// use par_iter_sync::IntoParallelIteratorSynced;
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
     fn into_par_iter_sync(self, func: F) -> ParIter<R>;
}

impl<R, T, TL, F> IntoParallelIteratorSynced<R, T, TL, F> for TL
    where
        F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
        T: Send,
        TL: Send + IntoIterator<Item = T>,
        <TL as IntoIterator>::IntoIter: Send + 'static,
        R: Send + 'static,
{
    fn into_par_iter_sync(self, func: F) -> ParIter<R>
    {
        ParIter::new(self, func)
    }
}

/// iterate through blocks according to array index.
pub struct ParIter<R> {
    /// Result receivers, one for each worker thread
    receivers: Vec<Receiver<R>>,
    /// Receiver<(task_number, thread)>
    task_order: Receiver<(usize, usize)>,
    /// current task number
    current: usize,
    /// handles to join worker threads
    worker_thread: Option<Vec<JoinHandle<()>>>,
    /// flag to stop workers from fetching new tasks
    iterator_stopper: Arc<AtomicBool>,
    /// indicate that workers have all been killed
    is_killed: bool,
}

impl<R> ParIter<R>
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
        // worker master
        let (task_register, task_order) = channel();
        let tasks = Arc::new(Mutex::new(tasks.into_iter().enumerate()));
        let mut handles = Vec::with_capacity(cpus);
        let mut receivers = Vec::with_capacity(cpus);
        for thread_number in 0..cpus {
            let (sender, receiver) = sync_channel(MAX_SIZE_FOR_THREAD);
            let task = tasks.clone();
            let register = task_register.clone();
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

        ParIter {
            receivers,
            task_order,
            current: 0,
            worker_thread: Some(handles),
            iterator_stopper,
            is_killed: false,
        }
    }
}

impl<R> ParIter<R> {
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
                    Ok((_, thread_number)) => self.receivers.get(thread_number).unwrap().recv(),
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
fn get_task<T, TL>(
    tasks: &Arc<Mutex<Enumerate<TL>>>,
    register: &Sender<(usize, usize)>,
    thread_number: usize,
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
        Some((task_number, task)) => {
            register.send((task_number, thread_number)).unwrap();
            Some(task)
        }
        None => None,
    }
}

impl<R> Iterator for ParIter<R> {
    type Item = R;

    ///
    /// The output API, use next to fetch result from the iterator.
    ///
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_killed {
            return None;
        }
        match self.task_order.recv() {
            Ok((task_number, thread_number)) => {
                // Some threads might have stopped first.
                // while the remaining working threads produces wrong order.
                if task_number != self.current {
                    self.kill();
                    return None;
                }

                match self.receivers.get(thread_number).unwrap().recv() {
                    Ok(block) => {
                        self.current += 1;
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

impl<R> ParIter<R> {
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

impl<R> Drop for ParIter<R> {
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
    use crate::IntoParallelIteratorSynced;

    fn error_at_1000(test_vec: &Vec<i32>, a: i32) -> Result<i32, ()> {
        let n = test_vec.get(a as usize).unwrap().to_owned();
        if n == 1000 {
            Err(())
        } else {
            Ok(n)
        }
    }

    #[test]
    fn par_iter() {

    }

    #[test]
    fn par_iter_test_exception() {
        let resource_captured = vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3];
        let results_expected = vec![3, 1, 4, 1];

        // if Err(()) is returned, the iterator stops early
        let results: Vec<i32> = (0..resource_captured.len()).into_par_iter_sync(move |a| {
            let n = resource_captured.get(a).unwrap().to_owned();
            if n == 5 {
                Err(())
            } else {
                Ok(n)
            }
        }).collect();

        assert_eq!(results, results_expected)
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
        let resource_captured: Vec<i32> = (0..10000).collect();
        let resource_captured_1 = resource_captured.clone();
        let resource_captured_2 = resource_captured.clone();
        let results_expected: Vec<i32> = (0..1000).collect();

        let results: Vec<i32> = (0..resource_captured.len()).into_par_iter_sync(move |a| {
            Ok(resource_captured.get(a).unwrap().to_owned())
        }).into_par_iter_sync(move |a| {
            error_at_1000(&resource_captured_1, a)
        }).into_par_iter_sync(move |a| {
            Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
        }).collect();

        assert_eq!(results, results_expected)
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
        let resource_captured: Vec<i32> = (0..10000).collect();
        let resource_captured_1 = resource_captured.clone();
        let resource_captured_2 = resource_captured.clone();
        let results_expected: Vec<i32> = (0..1000).collect();

        let results: Vec<i32> = (0..resource_captured.len()).into_par_iter_sync(move |a| {
            Ok(resource_captured.get(a).unwrap().to_owned())
        }).into_par_iter_sync(move |a| {
            Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
        }).into_par_iter_sync(move |a| {
            error_at_1000(&resource_captured_1, a)
        }).collect();

        assert_eq!(results, results_expected)
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
        let resource_captured: Vec<i32> = (0..10000).collect();
        let resource_captured_1 = resource_captured.clone();
        let resource_captured_2 = resource_captured.clone();
        let results_expected: Vec<i32> = (0..1000).collect();

        let results: Vec<i32> = (0..resource_captured.len()).into_par_iter_sync(move |a| {
            error_at_1000(&resource_captured_1, a as i32)
        }).into_par_iter_sync(move |a| {
            Ok(resource_captured.get(a as usize).unwrap().to_owned())
        }).into_par_iter_sync(move |a| {
            Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
        }).collect();

        assert_eq!(results, results_expected)
    }

    #[test]
    fn test_break() {
        let mut count = 0;
        for i in (0..20).into_par_iter_sync(|a| Ok(a)) {
            if i == 10 {
                break;
            }
            count += 1;
        }
        assert_eq!(count, 10)
    }
}
