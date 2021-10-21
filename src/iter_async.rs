use crate::MAX_SIZE_FOR_THREAD;
use crossbeam::channel;
use crossbeam::channel::Receiver;
use num_cpus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

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
    /// Result receivers, one for each worker thread
    receiver: Receiver<R>,
    /// handles to join worker threads
    worker_thread: Option<Vec<JoinHandle<()>>>,
    /// flag to stop workers from fetching new tasks
    iterator_stopper: Arc<AtomicBool>,
    /// indicate that workers have all been killed
    is_killed: bool,
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
        // worker master
        let (prepare, task) = channel::bounded(MAX_SIZE_FOR_THREAD * cpus);
        // prepare tasks
        let sender_thread = thread::spawn(move || {
            for t in tasks {
                if prepare.send(t).is_err() {
                    break;
                }
            }
        });
        let mut handles = Vec::with_capacity(cpus + 1);
        let (sender, receiver) = channel::bounded(MAX_SIZE_FOR_THREAD * cpus);
        for _ in 0..cpus {
            let task = task.clone();
            let iterator_stopper = iterator_stopper.clone();
            let task_executor = task_executor.clone();
            let sender = sender.clone();

            // workers
            let handle = thread::spawn(move || {
                loop {
                    if iterator_stopper.load(Ordering::SeqCst) {
                        break;
                    }
                    match get_task(&task) {
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
            handles.push(handle);
        }
        handles.push(sender_thread);

        ParIterAsync {
            receiver,
            worker_thread: Some(handles),
            iterator_stopper,
            is_killed: false,
        }
    }
}

impl<R> ParIterAsync<R> {
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
                // wait until all workers have stopped
                if self.receiver.recv().is_err() {
                    break;
                }
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
        match self.receiver.recv() {
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
    extern crate test;
    use crate::IntoParallelIteratorAsync;
    use std::collections::HashSet;
    use test::Bencher;

    #[test]
    fn par_iter_test_exception() {
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

    #[test]
    fn test_break() {
        let mut count = 0;
        for i in (0..20).into_par_iter_async(|a| Ok(a)) {
            if i == 10 {
                break;
            }
            count += 1;
        }
        assert_eq!(count, 10)
    }

    #[bench]
    fn bench_into_par_iter_async(b: &mut Bencher) {
        b.iter(|| {
            (0..1_000_000)
                .into_par_iter_async(|a| Ok(a))
                .for_each(|_| {})
        });
    }
}
