use std::env;
use std::time::Instant;

use chrono::{DateTime, Utc};
use sysinfo::System;

use tempfile::NamedTempFile;
use txn_engine::engine::Engine;
use txn_engine::utility::{generate_random_transactions, get_current_memory};

/// Entry point of the application. Parses command-line arguments to determine the mode of operation
/// (normal processing or stress testing) and handles transaction processing accordingly.
///
/// # Modes
/// - **Normal mode**: Processes transactions from a specified CSV file and outputs the resulting account states.
///   Optionally, the session state can be dumped after processing by including the `-dump` flag.
/// - **Stress test mode**: Generates and processes a specified number of random transactions to test the engine's performance.
///
/// # Command-line Usage
/// - Normal mode: `cargo run -- transactions.csv [-dump] > accounts.csv`
/// - Stress test mode: `cargo run -- stress-test <number_of_transactions> > accounts.csv`
///
/// # Returns
/// - `Ok(())` if processing completes successfully.
/// - `Err(Box<dyn std::error::Error>)` if any errors occur, such as incorrect arguments or processing failures.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 4 {
        eprintln!("Usage:");
        eprintln!("  Normal mode:     cargo run -- transactions.csv [-dump] > accounts.csv");
        eprintln!(
            "  Stress test mode: cargo run -- stress-test <number_of_transactions> > accounts.csv"
        );
        return Err("Incorrect number of arguments".into());
    }

    let mut engine = Engine::default();

    if args[1] == "stress-test" {
        if args.len() != 3 {
            return Err("Stress test requires a number of transactions to generate".into());
        }
        let num_transactions: usize = args[2].parse()?;
        process_stress_test(num_transactions)?;
    } else {
        let input_path = &args[1];
        process_normal(&mut engine, input_path, args.contains(&"-dump".to_string()))?;
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
    match engine.read_and_process_csv_file(input_path) {
        Ok(()) => {}
        Err(e) => eprintln!("Error: {}", e),
    }

    engine.output_results(std::io::stdout(), engine)?;

    if should_dump {
        let now: DateTime<Utc> = Utc::now();
        let timestamp = now.format("%Y%m%d_%H%M%S").to_string();

        let transactions_file = format!("{}_transaction_log.csv", timestamp);

        engine.dump_transaction_log_to_csvs(&transactions_file)?;
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
    let mut engine = Engine::default();
    let start_time = Instant::now();
    let mut system = System::new_all();
    let start_memory = get_current_memory(&mut system);

    // Use NamedTempFile for automatic cleanup
    // The temporary file is automatically deleted when temp_file goes out of scope
    let temp_file = NamedTempFile::new()?;
    generate_random_transactions(num_transactions, &temp_file)?;

    // Process transactions directly from the temporary file
    match engine.read_and_process_csv_file(temp_file.path().to_str().unwrap()) {
        Ok(()) => {}
        Err(e) => eprintln!("Error during stress test: {}", e),
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

    engine.output_results(std::io::stdout(), &engine)?;

    Ok(())
}
