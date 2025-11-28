use std::env;
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use sysinfo::System;

use tempfile::NamedTempFile;
use txn_engine::asyncengine::{AsyncEngine, AsyncEngineFunctions};
use txn_engine::engine::{Engine, EngineFunctions};
use txn_engine::utility::{generate_random_transactions, get_current_memory};

const BUFFER_SIZE: usize = 16_384;

/// Main entry point of the transaction engine.
///
/// The transaction engine processes transactions from a provided CSV file and updates account states accordingly.
///
/// Sync mode supports normal processing and stress testing modes.
///
/// Async mode supports normal processing and stress testing modes.
///
/// Sync usage:
///   Normal mode:     cargo run -- transactions.csv [-dump] > accounts.csv
///   Stress test mode: cargo run -- stress-test <number_of_transactions> > accounts.csv
///
/// Async usage:
///   Normal mode:     cargo run -- async transactions.csv [-dump] > accounts.csv
///   Stress test mode: cargo run -- async stress-test <number_of_transactions> > accounts.csv
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 5 {
        eprintln!("Sync Usage:");
        eprintln!("  Normal mode:     cargo run -- transactions.csv [-dump] > accounts.csv");
        eprintln!(
            "  Stress test mode: cargo run -- stress-test <number_of_transactions> > accounts.csv"
        );

        eprintln!("Async Usage:");
        eprintln!("  Normal mode:     cargo run -- async transactions.csv [-dump] > accounts.csv");
        eprintln!("  Stress test mode: cargo run -- async stress-test <number_of_transactions> > accounts.csv");

        return Err("Incorrect number of arguments".into());
    }

    match args[1].as_str() {
        "async" => {
            // async mode
            eprintln!("Async mode ");
            if args.len() <= 2 {
                eprintln!("Usage:");
                eprintln!(
                    "  Normal mode:     cargo run -- async transactions.csv [-dump] > accounts.csv"
                );
                eprintln!("  Stress test mode: cargo run -- async stress-test <number_of_transactions> > accounts.csv");
                return Err("Insufficient arguments for async mode".into());
            }
            eprint!("{} ", args[2]);
            let tokio_runtime =  tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
            if args[2] == "stress-test" {
                // asyc stress test mode
                eprintln!("stress test mode ");
                if args.len() != 4 {
                    return Err("Stress test requires a number of transactions to generate".into());
                }
                let num_transactions: usize = args[3].parse()?;
               
                tokio_runtime.block_on(async {
                    match process_stress_test_async(num_transactions).await {
                        Ok(()) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    };
                });
            } else {
                // normal async processing
                tokio_runtime.block_on(async {
                    let input_path = &args[2];
                    let mut engine = AsyncEngine::default();
                    match process_normal_async(
                        &mut engine,
                        input_path,
                        args.contains(&"-dump".to_string()),
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    };
                });
            }
        }

        _ => {
            // sync mode
            if args[1] == "stress-test" {
                // sync stress test mode
                if args.len() != 3 {
                    return Err("Stress test requires a number of transactions to generate".into());
                }
                let num_transactions: usize = args[2].parse()?;
                process_stress_test(num_transactions)?;
            } else {
                // normal sync processing
                let input_path = &args[1];
                let mut engine = Engine::default();
                process_normal(&mut engine, input_path, args.contains(&"-dump".to_string()))?;
            }
        }
    }

    Ok(())
}

/// Process transactions from a CSV file and optionally dump the session state.
///
/// # Parameters
/// - `engine`: Mutable reference to the Engine that processes transactions.
/// - `input_path`: Path to the CSV file containing transactions.
/// - `should_dump`: Boolean indicating whether to dump the session state after processing.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while reading from the file, processing transactions, or writing the dump.
fn process_normal(
    engine: &mut Engine,
    input_path: &str,
    should_dump: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {}
        Err(e) => eprintln!("Error: {}", e),
    }

    engine.dump_account_to_csv(std::io::stdout(), BUFFER_SIZE)?;

    if should_dump {
        let now: DateTime<Utc> = Utc::now();
        let timestamp = now.format("%Y%m%d_%H%M%S").to_string();

        let transactions_file = format!("{}_transaction_log.csv", timestamp);

        engine.dump_transaction_log_to_csv(&transactions_file, BUFFER_SIZE)?;
    }

    Ok(())
}

/// Process a specified number of random transactions and print performance metrics.
///
/// # Parameters
/// - `num_transactions`: The number of random transactions to generate and process.
/// - `start_memory`: The current memory consumption at the start of the stress test.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while generating or processing transactions.
///
/// # Notes
/// - The temporary file is automatically deleted when the function returns.
/// - The performance metrics are printed to stderr.
fn process_stress_test(num_transactions: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Use NamedTempFile for automatic cleanup
    // The temporary file is automatically deleted when temp_file goes out of scope
    let temp_file = NamedTempFile::new()?;
    generate_random_transactions(num_transactions, &temp_file)?;

    let mut engine = Engine::default();
    let start_time = Instant::now();
    let mut system = System::new_all();
    let start_memory = get_current_memory(&mut system);

    // Process transactions directly from the temporary file
    // Error are not printed on the stderr during the stress test as it may affect the performance of the engine
    // especially when the transactions are generated randomly and the error rate is is very high
    if let Ok(()) = engine
        .read_and_process_transactions_from_csv(temp_file.path().to_str().unwrap(), BUFFER_SIZE)
    {
    }

    {
        // let's measure the resoruces before creating the dump to properly measure the engine performance:
        let elapsed_time = start_time.elapsed();
        let memory_delta = get_current_memory(&mut system).saturating_sub(start_memory);

        let memory_delta_mb = (memory_delta as f64) / (1024.0 * 1024.0);
        let engine_memory_mb = (engine.size_of() as f64) / (1024.0 * 1024.0);
        eprintln!("Elapsed time: {:?}", elapsed_time);
        eprintln!("Engine Memory size: {:.3} MB", engine_memory_mb);
        eprintln!("Memory consumption delta: {:.3} MB", memory_delta_mb);
    }

    engine.dump_account_to_csv(std::io::stdout(), BUFFER_SIZE)?;

    Ok(())
}

/// Process transactions from a CSV file and optionally dump the session state.
///
/// # Parameters
/// - `engine`: Mutable reference to the AsycEngine that processes transactions.
/// - `input_path`: Path to the CSV file containing transactions.
/// - `should_dump`: Boolean indicating whether to dump the session state after processing.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while reading from the file, processing transactions, or writing the dump.
///
/// # Notes
/// - This function is asynchronous and returns a Future that resolves to a Result.
async fn process_normal_async(
    engine: &mut AsyncEngine,
    input_path: &str,
    should_dump: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Arc::new(engine);
    match engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await
    {
        Ok(()) => {}
        Err(e) => eprintln!("Error: {}", e),
    }

    match engine
        .dump_account_to_csv(tokio::io::stdout(), BUFFER_SIZE)
        .await
    {
        Ok(()) => {}
        Err(e) => eprintln!("Error: {}", e),
    };

    if should_dump {
        let now: DateTime<Utc> = Utc::now();
        let timestamp = now.format("%Y%m%d_%H%M%S").to_string();

        let transactions_file = format!("{}_transaction_log.csv", timestamp);

        match engine
            .dump_transaction_log_to_csv(&transactions_file, BUFFER_SIZE)
            .await
        {
            Ok(()) => {}
            Err(e) => eprintln!("Error: {}", e),
        };
    }

    Ok(())
}

/// Process a specified number of random transactions asynchronously and print performance metrics.
///
/// # Parameters
/// - `num_transactions`: The number of random transactions to generate and process.
/// - `start_memory`: The current memory consumption at the start of the stress test.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while generating or processing transactions.
///
/// # Notes
/// - The temporary file is automatically deleted when the function returns.
/// - The performance metrics are printed to stderr.
/// - This function is `async` and must be awaited or executed within an `async` context.
async fn process_stress_test_async(
    num_transactions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Use NamedTempFile for automatic cleanup
    // The temporary file is automatically deleted when temp_file goes out of scope
    let temp_file = NamedTempFile::new()?;
    generate_random_transactions(num_transactions, &temp_file)?;

    let engine = Arc::new(AsyncEngine::default());
    let start_time = Instant::now();
    let mut system = System::new_all();
    let start_memory = get_current_memory(&mut system);

    // Process transactions directly from the temporary file
    // Error are not printed on the stderr during the stress test as it may affect the performance of the engine
    // especially when the transactions are generated randomly and the error rate is is very high
    if let Ok(()) = engine
        .read_and_process_transactions_from_csv(temp_file.path().to_str().unwrap(), BUFFER_SIZE)
        .await
    {}

    {
        // let's measure the resoruces before creating the dump to properly measure the engine performance:
        let elapsed_time = start_time.elapsed();
        let memory_delta = get_current_memory(&mut system).saturating_sub(start_memory);

        let memory_delta_mb = (memory_delta as f64) / (1024.0 * 1024.0);
        let engine_memory_mb = (engine.size_of().await as f64) / (1024.0 * 1024.0);
        eprintln!("Elapsed time: {:?}", elapsed_time);
        eprintln!("Engine Memory size: {:.3} MB", engine_memory_mb);
        eprintln!("Memory consumption delta: {:.3} MB", memory_delta_mb);
    }

    match engine
        .dump_account_to_csv(tokio::io::stdout(), BUFFER_SIZE)
        .await
    {
        Ok(()) => {}
        Err(e) => eprintln!("Error: {}", e),
    };

    Ok(())
}
