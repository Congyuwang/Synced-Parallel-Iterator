use crate::MAX_SIZE_FOR_THREAD;
use crossbeam::channel;
use crossbeam::channel::Receiver;
use num_cpus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

///
/// This trait implement the async version of `IntoParallelIteratorSync`
///
pub trait IntoParallelIteratorAsync<R, T, TL, F>
where
    F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
    T: Send + 'static,
    TL: Send + IntoIterator<Item = T> + 'static,
    R: Send,
{
    ///
    /// An asynchronous equivalent of into_par_iter_sync
    ///
    fn into_par_iter_async(self, func: F) -> ParIterAsync<R>;
}

impl<R, T, TL, F> IntoParallelIteratorAsync<R, T, TL, F> for TL
where
    F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
    T: Send + 'static,
    TL: Send + IntoIterator<Item = T> + 'static,
    R: Send + 'static,
{
    fn into_par_iter_async(self, func: F) -> ParIterAsync<R> {
        ParIterAsync::new(self, func)
    }
}

/// iterate through blocks according to array index.
pub struct ParIterAsync<R> {
    /// this receiver receives results produced by workers
    output_receiver: Receiver<R>,

    /// handles to join worker threads
    worker_thread: Option<Vec<JoinHandle<()>>>,

    /// atomic flag to stop workers from fetching new tasks
    iterator_stopper: Arc<AtomicBool>,

    /// if this is `true`, it must guarantee that all worker threads have stopped
    is_killed: bool,

    /// number of worker threads
    worker_count: usize,
}

impl<R> ParIterAsync<R>
where
    R: Send + 'static,
{
    ///
    /// the worker threads are dispatched in this `new` constructor!
    ///
    pub fn new<T, TL, F>(tasks: TL, task_executor: F) -> Self
    where
        F: Send + Clone + 'static + Fn(T) -> Result<R, ()>,
        T: Send + 'static,
        TL: Send + IntoIterator<Item = T> + 'static,
    {
        let cpus = num_cpus::get();
        let iterator_stopper = Arc::new(AtomicBool::new(false));
        let stopper_clone = iterator_stopper.clone();

        // this thread dispatches tasks to worker threads
        let (dispatcher, task_receiver) = channel::bounded(MAX_SIZE_FOR_THREAD * cpus);
        let work_dispatcher = thread::spawn(move || {
            for t in tasks {
                if dispatcher.send(t).is_err() {
                    break;
                }
            }
        });

        // output senders for worker threads, and output receiver for user thread
        let (output_sender, output_receiver) = channel::bounded(MAX_SIZE_FOR_THREAD * cpus);

        // this is what each worker do
        let worker_task = move || {
            loop {
                // check stopper flag, stop if `true`
                if iterator_stopper.load(Ordering::SeqCst) {
                    break;
                }

                // fetch next task
                match get_task(&task_receiver) {
                    // break if no more task
                    None => break,
                    Some(task) => match task_executor(task) {
                        Ok(blk) => {
                            // send output
                            output_sender.send(blk).unwrap();
                        }
                        Err(_) => {
                            // stop other workers if error is returned
                            iterator_stopper.fetch_or(true, Ordering::SeqCst);
                            break;
                        }
                    },
                }
            }
        };

        // spawn worker threads
        let mut worker_handles = Vec::with_capacity(cpus + 1);
        for _ in 0..cpus {
            worker_handles.push(thread::spawn(worker_task.clone()));
        }
        worker_handles.push(work_dispatcher);

        ParIterAsync {
            output_receiver,
            worker_thread: Some(worker_handles),
            iterator_stopper: stopper_clone,
            is_killed: false,
            worker_count: cpus,
        }
    }
}

impl<R> ParIterAsync<R> {
    ///
    /// - stop workers from fetching new tasks
    /// - pull one result from each worker to prevent `send` blocking
    ///
    pub fn kill(&mut self) {
        if !self.is_killed {
            // stop threads from getting new tasks
            self.iterator_stopper.fetch_or(true, Ordering::SeqCst);
            // receive one for each channel to prevent blocking
            for _ in 0..self.worker_count {
                let _ = self.output_receiver.try_recv();
            }
            // all workers should reasonably stopped by now
            self.is_killed = true;
        }
    }
}

///
/// A helper function to receive task from task receiver.
///
/// It guarantees to return None if and only if there is no more new task.
///
#[inline(always)]
fn get_task<T>(tasks: &channel::Receiver<T>) -> Option<T>
where
    T: Send,
{
    // lock task list
    tasks.recv().ok()
}

impl<R> Iterator for ParIterAsync<R> {
    type Item = R;

    ///
    /// The output API, use next to fetch result from the iterator.
    ///
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_killed {
            return None;
        }
        match self.output_receiver.recv() {
            Ok(block) => Some(block),
            // all workers have stopped
            Err(_) => {
                self.kill();
                None
            }
        }
    }
}

impl<R> ParIterAsync<R> {
    ///
    /// Join worker threads. This can be only called only once.
    /// Otherwise it will panic.
    /// This is automatically called in `drop()`
    ///
    fn join(&mut self) {
        for handle in self.worker_thread.take().unwrap() {
            handle.join().unwrap()
        }
    }
}

impl<R> Drop for ParIterAsync<R> {
    ///
    /// Stop worker threads, join the threads.
    ///
    fn drop(&mut self) {
        self.kill();
        self.join();
    }
}

#[cfg(test)]
mod test_par_iter_async {
    #[cfg(feature = "bench")]
    extern crate test;
    use crate::IntoParallelIteratorAsync;
    use std::collections::HashSet;
    #[cfg(feature = "bench")]
    use test::Bencher;

    #[test]
    fn par_iter_test_exception() {
        for _ in 0..100 {
            let resource_captured = vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3];

            // if Err(()) is returned, the iterator stops early
            let results: HashSet<i32> = (0..resource_captured.len())
                .into_par_iter_async(move |a| {
                    let n = resource_captured.get(a).unwrap().to_owned();
                    if n == 5 {
                        Err(())
                    } else {
                        Ok(n)
                    }
                })
                .collect();

            assert!(!results.contains(&5))
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

            let results: HashSet<i32> = (0..resource_captured.len())
                .into_par_iter_async(move |a| Ok(resource_captured.get(a).unwrap().to_owned()))
                .into_par_iter_async(move |a| {
                    let n = resource_captured_1.get(a as usize).unwrap().to_owned();
                    if n == 1000 {
                        Err(())
                    } else {
                        Ok(n)
                    }
                })
                .into_par_iter_async(move |a| {
                    Ok(resource_captured_2.get(a as usize).unwrap().to_owned())
                })
                .collect();

            assert!(!results.contains(&1000))
        }
    }

    #[test]
    /// test that the iterator won't deadlock during drop
    fn test_break() {
        for _ in 0..10000 {
            for i in (0..2000).into_par_iter_async(|a| Ok(a)) {
                if i == 1000 {
                    break;
                }
            }
        }
    }

    #[cfg(feature = "bench")]
    #[bench]
    fn bench_into_par_iter_async(b: &mut Bencher) {
        b.iter(|| {
            (0..1_000_000)
                .into_par_iter_async(|a| Ok(a))
                .for_each(|_| {})
        });
    }
}
