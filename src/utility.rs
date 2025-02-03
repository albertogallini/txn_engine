use crate::{datastr::transaction::TransactionProcessingError, engine::Engine};
use rand::{thread_rng, Rng};
use std::{fs::File, io::BufReader, process};
use sysinfo::{Pid, System};
use tempfile::NamedTempFile;

/// Reads transactions from a CSV file and processes them.
///
/// # Parameters
/// - `engine`: The Engine instance to process transactions with.
/// - `input_path`: The path to the CSV file to read transactions from.
///
/// # Returns
/// - `Ok(())` if all transactions are processed without errors.
/// - `Err(TransactionProcessingError)` if any errors occur during processing or reading.
pub fn read_and_process_csv_file(
    engine: &mut Engine,
    input_path: &str,
) -> Result<(), TransactionProcessingError> {
    const BATCH_SIZE: usize = 16_384;

    let file = File::open(input_path).map_err(|e| {
        TransactionProcessingError::MultipleErrors(vec![format!("Error opening file: {}", e)])
    })?;
    let reader = BufReader::new(file);

    // Call the method from the Engine struct
    engine.read_and_process_transactions(reader, BATCH_SIZE)
}

/// Generates a specified number of random transactions and writes them to a temporary CSV file.
///
/// This function is used for stress testing purposes.
///
/// # Parameters
/// - `num_transactions`: The number of transactions to generate.
/// - `temp_file`: A temporary file created with `NamedTempFile::new()`. This file will be overwritten.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while writing to the file.
pub fn generate_random_transactions(
    num_transactions: usize,
    temp_file: &NamedTempFile,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(temp_file.path())?;
    let mut writer = csv::Writer::from_writer(file);
    let mut rng = thread_rng();
    writer.write_record(["type", "client", "tx", "amount"])?;
    for _ in 0..num_transactions {
        let ty = match rand::random::<u8>() % 5 {
            0 => "deposit",
            1 => "withdrawal",
            2 => "dispute",
            3 => "resolve",
            _ => "chargeback",
        };
        let client = (rng.gen_range(0.0..1_000_000.0) + 1.) as u16; // To ensure client ID starts from 1
        let tx = (rng.gen_range(0.0..10_000_000.0) + 1.) as u32; // To ensure tx ID starts from 1
        let amount = if ty == "dispute" || ty == "resolve" || ty == "chargeback" {
            "".to_string()
        } else {
            format!("{:.4}", rng.gen_range(0.0..100_000.0))
        };
        writer.write_record([ty, &client.to_string(), &tx.to_string(), &amount])?;
    }
    writer.flush()?;
    Ok(())
}

/// Retrieves the memory usage of the current process.
///
/// This function refreshes the system's process information to find and return
/// the memory used by the current process.
///
/// # Parameters
/// - `system`: A mutable reference to a `System` instance that will be refreshed
///   to obtain up-to-date process information.
///
/// # Returns
/// - `u64`: The memory consumption of the current process, in kilobytes.
pub fn get_current_memory(system: &mut System) -> u64 {
    system.refresh_all();
    let current_pid = process::id(); // process::id() returns a u32

    system
        .processes()
        .get(&Pid::from_u32(current_pid))
        .map(|process| process.memory())
        .unwrap_or(0)
}
