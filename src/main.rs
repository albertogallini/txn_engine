use std::env;
use std::time::Instant;

use csv::Writer;
use sysinfo::System;

use tempfile::NamedTempFile;
use txn_engine::datastr::account::write_account_balances;
use txn_engine::engine::Engine;
use txn_engine::utility::{
    generate_random_transactions, get_current_memory, read_and_process_csv_file,
};

/// The main entry point for the command-line interface.
///
/// The program can be run in two modes:
///
/// 1. Normal mode: `cargo run -- transactions.csv > accounts.csv`
///    Reads transactions from a CSV file and processes them using the Engine.
///    Writes the resulting accounts to stdout as CSV.
///
/// 2. Stress test mode: `cargo run -- stress-test <number_of_transactions> > accounts.csv`
///    Generates a specified number of random transactions and processes them using the Engine.
///    Writes the resulting accounts to stdout as CSV.
///
/// The program returns an error if the number of arguments is incorrect.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: cargo run -- transactions.csv > accounts.csv");
        eprintln!(
            "Or for stress test: cargo run -- stress-test <number_of_transactions> > accounts.csv"
        );
        return Err("Incorrect number of arguments".into());
    }

    if args[1] == "stress-test" {
        if args.len() != 3 {
            return Err("Stress test requires a number of transactions to generate".into());
        }
        let num_transactions: usize = args[2].parse()?;
        process_stress_test(num_transactions)?;
    } else {
        let input_path = &args[1];
        process_normal(input_path)?;
    }

    Ok(())
}

/// Process transactions from a CSV file and write the resulting accounts to stdout as CSV.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while reading from the file or processing transactions.
fn process_normal(input_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }
    output_results(&engine)
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
    match read_and_process_csv_file(&mut engine, temp_file.path().to_str().unwrap()) {
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

    output_results(&engine)?;

    Ok(())
}

/// Writes the final state of all accounts to stdout as a CSV file.
///
/// This function leverages `write_account_balances` to handle the writing
/// of account information in CSV format. It begins by writing the CSV header,
/// followed by the account data. The order of the columns is:
/// - client: The client ID.
/// - available: The available balance for the client.
/// - held: The held balance for the client.
/// - total: The total balance for the client.
/// - locked: Whether the account is locked.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while writing to stdout.
fn output_results(engine: &Engine) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = Writer::from_writer(std::io::stdout());
    writer.write_record(["client", "available", "held", "total", "locked"])?;
    writer.flush()?; // Ensure the header is written before calling write_account_balances

    write_account_balances(&engine.accounts)
}
