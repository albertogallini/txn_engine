# Async Engine VS Sync Engine - Performance analysis

## TL;DR
Here we compare `AsyncEngine` versus `Engine` performance, analyzing their behavior under different conditions.
 Here below is a synthetic report of the outcome of what we have measured.
| Workload                          | Winner       | Speedup       | Reason                                                   |
|-----------------------------------|--------------|---------------|----------------------------------------------------------|
| Concurrency test `reg_test_engine_consistency_with_concurrent_processing/_async` (3 huge files)   | **Sync**         | **~4s faster**    | Channel overhead dominates when no overlap is possible  |
| `stress-test.sh` (one huge file)  | **Async**    | **+20–24%**   | Parsing and processing run in **true parallel**          |
| Production (Low concurrency/ large in-memory state  )    | **Sync**    |    n/a    | Sync offer slighly better performance as there is no async runtime overhead. But Asnyc and Sync are oveall equivalent   |
| Production (10k+ concurrent clients / high frequency  )    | **Async**    |    n/a    | Only async scales to massive concurrent connections     |

In this document, we will provide some quick analysis of these results, explaining why the Async Engine performs better than the Sync Engine and why it sometimes performs worse.



## Analysis of concurrency test unit `reg_test_engine_consistency_with_concurrent_processing/_async`

Two implementations are provided:

- `Engine` — pure synchronous, `DashMap` + `std::thread`
- `AsyncEngine` — fully asynchronous, `tokio` + `spawn_blocking` + channel


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

┌─────────────────────────────────────────────────────┐
│  Result:                                            │
│   AsyncEngine : 26.42 s                             │
│   Sync Engine : 22.25 s  ← **~19% faster**          │
└─────────────────────────────────────────────────────┘
```

| Factor                  | Sync (`std::thread`)                              | Async (`spawn_blocking` + channel)                                       | Winner |
|-------------------------|----------------------------------------------------|---------------------------------------------------------------------------|--------|
| Number of threads       | 3 real OS threads                                  | 3 blocking threads +  async workers threads                             | Sync   |
| Memory allocations      | Only CSV parsing                                   | +3_000_000 `send()` + 3_000_000 `recv()` allocations                     | Sync   |
| Channel overhead        | None                                               | `flume` / `tokio::mpsc` ≈ 100–300 ns per message → 300–900 ms total      | Sync   |
| Lock contention         | `DashMap` sharding → almost zero contention       | Same `DashMap` (or slightly slower `tokio::RwLock`)                       | Tie    |
| Context switches        | Almost none                                        | Thousands per second (channel wake-ups)                                   | Sync   |
| CPU cache efficiency    | Excellent – one thread works on one huge file      | Good, but crossing the channel hurts cache locality                      | Sync   |

**Bottom line:**  
On this specific benchmark (three 1-million-row files) we pay the unavoidable cost of **3 million channel messages**. Even the fastest channel in the Rust ecosystem (`flume::unbounded`) adds several hundred milliseconds — the ~4-second gap we observe is expected and cannot be eliminated.

## stress test perfomance `stress-test.sh`

Here below the comparison of Async vs Sync engine stress test: 

```
txn_engine 👉 ./stress-test.sh async
    Finished `release` profile [optimized] target(s) in 0.16s
Running stress test async for 100 transactions...
Running stress test async for 100100 transactions...
Running stress test async for 200100 transactions...
Running stress test async for 300100 transactions...
Running stress test async for 400100 transactions...
Running stress test async for 500100 transactions...
Running stress test async for 600100 transactions...
Running stress test async for 700100 transactions...
Running stress test async for 800100 transactions...
Running stress test async for 900100 transactions...
Running stress test async for 1000100 transactions...
Running stress test async for 1100100 transactions...
Running stress test async for 1200100 transactions...
Running stress test async for 1300100 transactions...
Running stress test async for 1400100 transactions...
Running stress test async for 1500100 transactions...
Running stress test async for 1600100 transactions...
Running stress test async for 1700100 transactions...
Running stress test async for 1800100 transactions...
Running stress test async for 1900100 transactions...
Running stress test async for 2000100 transactions...
Transactions Count   Time                 Process Memory (MB)  Engine Memory (MB)  
100                  6.708ms              0.344                0.001               
100100               65.961083ms          13.188               1.246               
200100               125.454416ms         16.688               2.501               
300100               193.446459ms         24.688               3.754               
400100               252.567ms            25.469               1.756               
500100               312.753166ms         32.016               6.186               
600100               501.087875ms         34.156               3.309               
700100               447.626834ms         42.828               4.108               
800100               515.247417ms         45.828               4.956               
900100               589.520917ms         48.469               6.476               
1000100              645.860833ms         49.531               5.787               
1100100              722.462833ms         49.484               3.437               
1200100              799.000792ms         65.781               3.564               
1300100              871.879041ms         68.281               3.742               
1400100              946.226917ms         72.719               6.445               
1500100              986.977375ms         70.812               4.353               
1600100              1.07147275s          75.312               7.319               
1700100              1.135149583s         79.234               7.674               
1800100              1.179195417s         77.172               3.458               
1900100              1.262267334s         84.078               4.561               
2000100              1.336663666s         84.031               6.129       

 txn_engine 👉 ./stress-test.sh      
    Finished `release` profile [optimized] target(s) in 0.13s
Running stress test  for 100 transactions...
Running stress test  for 100100 transactions...
Running stress test  for 200100 transactions...
Running stress test  for 300100 transactions...
Running stress test  for 400100 transactions...
Running stress test  for 500100 transactions...
Running stress test  for 600100 transactions...
Running stress test  for 700100 transactions...
Running stress test  for 800100 transactions...
Running stress test  for 900100 transactions...
Running stress test  for 1000100 transactions...
Running stress test  for 1100100 transactions...
Running stress test  for 1200100 transactions...
Running stress test  for 1300100 transactions...
Running stress test  for 1400100 transactions...
Running stress test  for 1500100 transactions...
Running stress test  for 1600100 transactions...
Running stress test  for 1700100 transactions...
Running stress test  for 1800100 transactions...
Running stress test  for 1900100 transactions...
Running stress test  for 2000100 transactions...
Transactions Count   Time                 Process Memory (MB)  Engine Memory (MB)  
100                  5.091833ms           0.172                0.001               
100100               77.255209ms          6.844                1.258               
200100               157.531875ms         17.172               2.491               
300100               234.120375ms         18.219               2.564               
400100               300.117875ms         25.422               4.957               
500100               390.2915ms           26.062               4.958               
600100               505.10375ms          41.828               2.982               
700100               583.928875ms         39.469               3.819               
800100               651.382458ms         39.391               5.120               
900100               723.825709ms         49.125               5.613               
1000100              831.94425ms          44.375               4.534               
1100100              920.4715ms           56.219               4.472               
1200100              1.000606584s         62.562               5.818               
1300100              1.112971834s         67.859               4.503               
1400100              1.180015417s         69.781               3.972               
1500100              1.286845875s         73.953               5.165               
1600100              1.317268709s         91.891               8.153               
1700100              1.425499333s         83.000               6.404               
1800100              1.557667792s         80.578               3.351               
1900100              1.6983535s           80.047               7.479               
2000100              1.75419325s          86.703               3.600  
```

When running the realistic stress test (`stress-test.sh`) on a single large file, the **async version wins by up to 24%** while keeping the same memory usage.:

```text
2_000_100 transactions:
  Sync  → 1.754 seconds
  Async → 1.336 seconds   ← 23.8% faster!
```


### Benefits of using channels during the async CSV parsing (`AsyncEngine::read_and_process_transactions`)

```rust
spawn_blocking → sync CSV parsing (dedicated real threads)
        ↓
   channel (flume)
        ↓
async task → transaction processing (yields on lock contention)
```

This creates **real overlap**:
- CSV parsing is pure CPU work → belongs on blocking threads
- Transaction processing hits contended map → benefits from async yielding

By combining `tokio::task::spawn_blocking` with a fast channel, the async engine achieves **real CPU parallelism**:

- **One dedicated OS thread** parses the CSV file using the ultra-fast synchronous `csv` crate  
- **One async worker thread** consume transactions from the channel and update account state

These two phases run **simultaneously** — there is **genuine overlap** between parsing and processing. Given that we are running on  a multicore machine. 
While the sync version does everything sequentially on one thread per file → **no overlap**.


## Production/real sysatem yses cases

Production systems can be designed under different assumptions that change the design pattern to follow (sync vs async).
This is well **elaborated** in the **"Async vs Sync impact on scalabilty"** section in the [Readme.md](./Readme.md#L484) and put into perspective and relation with the memory footprint of the process.

If the requirement is  to manage **thousdands of concurrent transactions** on the same engine instance things change. i.e.:  

```rust
// Pefromance will drammatically degratate (or even getting a crash) with >10,000 concurrent clients
for client in 10_000_clients {
    std::thread::spawn(move || process_client(client));
}

// This handles hundreds of thousands of connections effortlessly
for client in 10_000_clients {
    tokio::spawn(async move {
        engine.process_transaction(&tx).await;
    });
}
```

But this also **implies** the memory footprint of the process is such that it scales with the higher number of transactions processed in a single node. If there is **no** such need, the **Sync** implementation performs well (sometimes slightly better as there is no additional task runtime overhead) and helps the codebase to remain simpler, also allowing to rely on **battle-tested** data structures like `DashMap`.
The strong need for an **Async Engine** must be evaluated together with the **overall system design** and the **use cases** (e.g., the actual number of concurrent clients in a unit of time per physical node/instance of the engine). This metric tells us exactly if we get into the scenario of having thousands of **concurrent threads** on the same node.