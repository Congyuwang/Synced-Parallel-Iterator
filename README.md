# par_iter_sync: Parallel Iterator With Sequential Output

Crate like `rayon` do not offer synchronization mechanism.
This crate provides easy mixture of parallelism and synchronization,
such as executing tasks in concurrency with synchronization in certain steps.

Consider the case where multiple threads share a cache which can be read
only after prior tasks have written to it (e.g., reads of task 4 depends
on writes of task 1-4).

Using `IntoParallelIteratorSync` trait
```rust
// in concurrency: task1 write | task2 write | task3 write | task4 write
//                      \_____________\_____________\_____________\
//             task4 read depends on task 1-4 write  \___________
//                                                               \
// in concurrency:              | task2 read  | task3 read  | task4 read

use par_iter_sync::IntoParallelIteratorSync;
use std::sync::{Arc, Mutex};
use std::collections::HashSet;

// there are 100 tasks
let tasks = 0..100;

// an in-memory cache for integers
let cache: Arc<Mutex<HashSet<i32>>> = Arc::new(Mutex::new(HashSet::new()));
let cache_clone = cache.clone();

// iterate through tasks
tasks.into_par_iter_sync(move |task_number| {

    // writes cache (write the integer in cache), in parallel
    cache.lock().unwrap().insert(task_number);
    // return the task number to the next iterator
    Ok(task_number)

}).into_par_iter_sync(move |task_number| { // <- synced to sequential order

    // reads
    assert!(cache_clone.lock().unwrap().contains(&task_number));
    Ok(())

// append a for each to actually run the whole chain
}).for_each(|_| ());
```

## Sequential Consistency
The output order is guaranteed to be the same as the upstream iterator,
but the execution order is not sequential.

## Examples

### Mix Syncing and Parallelism By Chaining
```rust
use par_iter_sync::IntoParallelIteratorSync;

(0..100).into_par_iter_sync(|i| {
    Ok(i)                     // <~ async execution
}).into_par_iter_sync(|i| { // <- sync order
    Ok(i)                     // <~async execution
}).into_par_iter_sync(|i| { // <- sync order
    Ok(i)                     // <~async execution
});                           // <- sync order
```

### Use `std::iter::IntoIterator` interface
```rust
use par_iter_sync::IntoParallelIteratorSync;

let mut count = 0;

// for loop
for i in (0..100).into_par_iter_sync(|i| Ok(i)) {
    assert_eq!(i, count);
    count += 1;
}

// sum
let sum: i32 = (1..=100).into_par_iter_sync(|i| Ok(i)).sum();

// take and collect
let results: Vec<i32> = (0..10).into_par_iter_sync(|i| Ok(i)).take(5).collect();

assert_eq!(sum, 5050);
assert_eq!(results, vec![0, 1, 2, 3, 4])
```

### Closure Captures Variables
Variables captured are cloned to each threads automatically.
```rust
use par_iter_sync::IntoParallelIteratorSync;
use std::sync::Arc;

// use `Arc` to save RAM
let resource_captured = Arc::new(vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3]);
let len = resource_captured.len();

let result_iter = (0..len).into_par_iter_sync(move |i| {
    // `resource_captured` is moved into the closure
    // and cloned to worker threads.
    let read_from_resource = resource_captured.get(i).unwrap();
    Ok(*read_from_resource)
});

// the result is produced in sequential order
let collected: Vec<i32> = result_iter.collect();
assert_eq!(collected, vec![3, 1, 4, 1, 5, 9, 2, 6, 5, 3])
```

### Fast Fail During Exception
The iterator stops once the inner function returns an `Err`.
```rust
use par_iter_sync::IntoParallelIteratorSync;
use std::sync::Arc;
use log::warn;

/// this function returns `Err` when it reads 1000
fn error_at_1000(n: i32) -> Result<i32, ()> {
    if n == 1000 {
        // you may log this error
        warn!("Some Error Occurs");
        Err(())
    } else {
        Ok(n)
    }
}

let results: Vec<i32> = (0..10000).into_par_iter_sync(move |a| {
    Ok(a)
}).into_par_iter_sync(move |a| {
    // error at 1000
    error_at_1000(a)
}).into_par_iter_sync(move |a| {
    Ok(a)
}).collect();

let expected: Vec<i32> = (0..1000).collect();
assert_eq!(results, expected)
```

#### You may choose to skip error
If you do not want to stop on `Err`, this is a workaround.
```rust
use par_iter_sync::IntoParallelIteratorSync;
use std::sync::Arc;

let results: Vec<Result<i32, ()>> = (0..5).into_par_iter_sync(move |n| {
    // error at 3, but skip
    if n == 3 {
        Ok(Err(()))
    } else {
        Ok(Ok(n))
    }
}).collect();

assert_eq!(results, vec![Ok(0), Ok(1), Ok(2), Err(()), Ok(4)])
```

## Implementation Note

### Output Buffering
- Each worker use a synced single-producer mpsc channel to buffer outputs.
  So, when a thread is waiting for its turn to get polled, it does not
  get blocked. The channel size is hard-coded to 10 for each thread.
- The number of threads equals to the number of logical cores.

### Synchronization Mechanism
- When each thread fetch a task, it registers its thread ID (`thread_number`)
  and the task ID (`task_number`) into a mpsc channel.
- When `next()` is called, the consumer fetch from the task registry
  (`task_order`) the next thread ID and task ID.
  It then receives from the channel of that thread, and checks whether
  the current task (`current`) matches the task ID to ensure that no thread
  has run into exception.
- If `next()` detect that some thread has not produced result due to exception,
  it calls `kill()`, which stop threads from fetching new tasks,
  flush remaining tasks, and joining the worker threads.

### Error handling and Dropping
- When any exception occurs, stop producers from fetching new task.
- Before dropping the structure, stop all producers from fetching tasks,
  flush all remaining tasks, and join all threads.
