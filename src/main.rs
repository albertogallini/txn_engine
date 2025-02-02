use csv::Writer;
use std::env;
use txn_engine::engine::{read_and_process_transactions, Engine};

/// This program takes a single command line argument, the path to a CSV file
/// containing transactions. The program processes the transactions and writes
/// the resulting account balances to stdout in CSV format.
///
/// The CSV file should have the following format:
/// type,       client, tx,    amount
/// deposit,    1,     1,      1.0
/// withdrawal, 1,     2,      1.5
/// dispute,    1,     1,
/// resolve,    1,     1,
/// chargeback, 1,     1,
///
/// The program will exit with a non-zero status if any error occurs while
/// processing the transactions.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: cargo run -- transactions.csv > accounts.csv");
        return Err("Incorrect number of arguments".into());
    }

    let input_path = &args[1];

    // Process transactions
    let mut engine = Engine::default();
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    // Write account balances to stdout
    let mut writer = Writer::from_writer(std::io::stdout());
    // Write headers
    writer.write_record(["client", "available", "held", "total", "locked"])?;
    for (client, account) in engine.accounts.iter() {
        writer.write_record(&[
            client.to_string(),
            account.available.to_string(),
            account.held.to_string(),
            account.total.to_string(),
            account.locked.to_string(),
        ])?;
    }
    writer.flush()?;

    Ok(())
}
