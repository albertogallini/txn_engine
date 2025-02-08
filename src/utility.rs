use csv::Writer;
use rand::{thread_rng, Rng};
use rust_decimal::prelude::*;
use std::{collections::HashMap, fs::File, process};
use sysinfo::{Pid, System};
use tempfile::NamedTempFile;

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

/// Generates a specified number of random transactions and writes them to a temporary CSV file.
///
/// The generated transactions will have a client ID between `start_client_id` and `end_client_id` (inclusive).
/// The generated transactions will have a transaction ID starting from `start_tx_id` and incrementing by 1 for each
/// transaction.
/// This is important to make sure
/// 1. to isolate the transactions for a group of clients
/// 2. to shard the transaction id space across concurrent streams.
///
/// The generated transactions will have a balance per client that is tracked and updated based on the type of transaction.
/// The balance per client is used to ensure that the generated transactions are valid, i.e. a withdrawal will not be
/// generated if the balance for the client is 0.
///
/// The generated transactions will be written to the temporary file in the order of deposit, withdrawal, dispute, resolve, chargeback.
/// The transactions will be written in the format `type,client,tx,amount`.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while writing to the file.
pub fn generate_random_transaction_concurrent_stream(
    num_transactions: usize,
    start_tx_id: u32,
    start_client_id: u16,
    end_client_id: u16,
) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let temp_file = NamedTempFile::new()?;
    let file = File::create(temp_file.path())?;
    let mut writer = Writer::from_writer(file);
    let mut rng = rand::thread_rng();

    writer.write_record(["type", "client", "tx", "amount"])?;

    // Track balance per client
    let mut client_balances = HashMap::new();
    let mut transactions = Vec::new();

    for i in 0..num_transactions {
        let ty;
        let mut amount = Decimal::ZERO;
        // Choose a random client ID
        let client = rng.gen_range(start_client_id..=end_client_id) as u16;

        // Get or initialize the balance for the client
        let mut balance = client_balances
            .entry(client)
            .or_insert(Decimal::ZERO.checked_add(Decimal::new(1, 1)))
            .unwrap();
        let mut tx = start_tx_id + (i as u32);

        match rand::random::<u8>() % 5 {
            0 => {
                ty = "deposit";
                amount = Decimal::new(rng.gen_range(1..1001), 2); // Random amount between 0.01 and 10.00
                balance += amount;
            }
            1 => {
                ty = "withdrawal";
                amount = Decimal::new(rng.gen_range(1..10), 2); // Random amount between 0.01 and 0.10
                if amount > balance {
                    amount = balance / Decimal::new(2, 2);
                }
                if amount == Decimal::ZERO {
                    continue;
                }
                balance -= amount;
            }
            2 => {
                ty = "dispute";
                tx = rng.gen_range(start_tx_id..=start_tx_id + (i as u32)) as u32;
            }
            3 => {
                ty = "resolve";
                tx = rng.gen_range(start_tx_id..=start_tx_id + (i as u32)) as u32;
            }
            _ => {
                ty = "chargeback";
                tx = rng.gen_range(start_tx_id..=start_tx_id + (i as u32)) as u32;
            }
        };

        transactions.push((
            ty.to_string(),
            client,
            tx, // Use the sequentially generated transaction ID
            amount,
        ));
    }

    for (ty, client, tx, amount) in transactions {
        if ty == "deposit" || ty == "withdrawal" {
            writer.write_record([
                ty,
                client.to_string(),
                tx.to_string(),
                format!("{:.4}", amount),
            ])?;
        } else {
            writer.write_record([ty, client.to_string(), tx.to_string(), "".to_string()])?;
        }
    }

    writer.flush()?;
    Ok(temp_file)
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
