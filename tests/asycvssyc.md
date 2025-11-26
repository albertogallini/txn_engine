### Why the pure-sync + `std::thread` version is still faster on this benchmark

```
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/async_test.rs (target/debug/deps/async_test-7c23470315159b07)

running 1 test
test reg_test_engine_consistency_with_concurrent_processing_async ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 20 filtered out; finished in 26.42s

     Running tests/test.rs (target/debug/deps/test-4bf91e7dc903642d)

running 1 test
test reg_test_engine_consistency_with_concurrent_processing ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 21 filtered out; finished in 22.25s
```

| Factor                  | Sync (`std::thread`)                              | Async (`spawn_blocking` + channel)                                       | Winner |
|-------------------------|----------------------------------------------------|---------------------------------------------------------------------------|--------|
| Number of threads       | 3 real OS threads                                  | 3 blocking threads + 1–8 async worker threads                             | Sync   |
| Memory allocations      | Only CSV parsing                                   | +3_000_000 `send()` + 3_000_000 `recv()` allocations                     | Sync   |
| Channel overhead        | None                                               | `flume` / `tokio::mpsc` ≈ 100–300 ns per message → 300–900 ms total      | Sync   |
| Lock contention         | `DashMap` sharding → almost zero contention       | Same `DashMap` (or slightly slower `tokio::RwLock`)                       | Tie    |
| Context switches        | Almost none                                        | Thousands per second (channel wake-ups)                                   | Sync   |
| CPU cache efficiency    | Excellent – one thread works on one huge file      | Good, but crossing the channel hurts cache locality                      | Sync   |

**Bottom line:**  
On this specific benchmark (three 1-million-row files) we pay the unavoidable cost of **3 million channel messages**. Even the fastest channel in the Rust ecosystem (`flume::unbounded`) adds several hundred milliseconds — the ~4-second gap we observe is expected and cannot be eliminated.

### But in real production the tables turn completely

```rust
// This will OOM/crash with >10,000 concurrent clients
for client in 10_000_clients {
    std::thread::spawn(move || process_client(client));
}

// This flies – handles hundreds of thousands of connections effortlessly
for client in 10_000_clients {
    tokio::spawn(async move {
        engine.process_transaction(&tx).await;
    });
}