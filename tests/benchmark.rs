use andb::LogStore;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::path::Path;

#[test]
fn bench_current_appends() {

    let log_path = "bench_sync_test.log";

    if Path::new(log_path).exists() {
        let _ = std::fs::remove_file(log_path);
    }

    let store = LogStore::open(log_path).unwrap();
    let shared_store = Arc::new(Mutex::new(store));


    let num_threads = 16;
    let appends_per_thread = 250;
    let total_appends = num_threads * appends_per_thread;

    println!("Starting benchmark: {} threads x {} appends = {} total writes...",
        num_threads, appends_per_thread, total_appends);

    let start = Instant::now();
    let mut handles = vec![];

    for t in 0..num_threads {
        let store_clone = Arc::clone(&shared_store);
        let handle = thread::spawn(move || {
            for i in 0..appends_per_thread {
                let payload = format!("thread {} record {}", t, i);
                let mut store = store_clone.lock().unwrap();
                store.append(payload.as_bytes()).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    println!("Benchmark complete in: {:?}", duration);
    println!("Average time per append: {:?}", duration / total_appends as u32);

    let _ = std::fs::remove_file(log_path);




}
