# Lock-free Sequential Parallel Iterator

## par_iter_sync

![rust test](https://github.com/Congyuwang/Synced-Parallel-Iterator/actions/workflows/rust.yml/badge.svg)

Crate like `rayon` do not offer synchronization mechanism.
This crate provides easy mixture of parallelism and synchronization.

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

## Usage Caveat
This crate is designed to clone all resources captured by the closure
for each thread. To prevent unintended RAM usage, you may wrap
large data structure using `Arc` (especially vectors of `Clone` objects).

## Sequential Consistency
The output order is guaranteed to be the same as the upstream iterator,
but the execution order is not sequential.

## Overhead Benchmark
Platform: Macbook Air (2015 Late) 8 GB RAM, Intel Core i5, 1.6GHZ (2 Core).

### Result
One million (1,000,000) empty iteration for each run.
```
test iter_async::test_par_iter_async::bench_into_par_iter_async ... bench: 120,003,398 ns/iter (+/- 52,401,527)
test test_par_iter::bench_into_par_iter_sync                    ... bench:  98,472,767 ns/iter (+/- 9,901,593)
```

Result:
- Async iterator overhead `120,003,398 / 1,000,000 = 120 ns (+/- 52 ns)`.
- Sync iterator overhead  ` 98,472,767 / 1,000,000 =  98 ns (+/- 10 ns)`.

### Bench Programs

#### iter_async
```rust
    #[bench]
    fn bench_into_par_iter_async(b: &mut Bencher) {
        b.iter(|| {
            (0..1_000_000).into_par_iter_async(|a| Ok(a)).for_each(|_|{})
        });
    }
```

#### iter_sync
```rust
    #[bench]
    fn bench_into_par_iter_sync(b: &mut Bencher) {
        b.iter(|| {
            (0..1_000_000).into_par_iter_sync(|a| Ok(a)).for_each(|_|{})
        });
    }
```

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
Variables captured are cloned to each thread automatically.
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
  get blocked. The channel size is hard-coded to 100 for each thread.
- The number of threads equals to the number of logical cores.

### Synchronization and Exception Handling
- When each thread fetch a task, it registers its thread ID and task ID into a registry.
- When `next()` is called, the consumer fetch from the task registry the next thread ID.
- `next()` returns None if there is no more task or if some Error occurs.
