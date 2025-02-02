use csv::Writer;
use rand::{thread_rng, Rng};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;
use sysinfo::System;
use txn_engine::engine::{read_and_process_transactions, Engine};

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
        let temp_file = "temp.csv";
        generate_random_transactions(num_transactions, temp_file)?;
        let mut system = System::new_all();
        let start_memory = get_current_memory(&mut system);
        process_stress_test(temp_file, start_memory)?;
    } else {
        let input_path = &args[1];
        process_normal(input_path)?;
    }

    Ok(())
}

fn generate_random_transactions(
    num_transactions: usize,
    file_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);
    let mut rng = thread_rng();

    writeln!(writer, "type,client,tx,amount")?;
    for _ in 0..num_transactions {
        let ty = match rng.gen_range(0..5) {
            0 => "deposit",
            1 => "withdrawal",
            2 => "dispute",
            3 => "resolve",
            _ => "chargeback",
        };
        let client = rng.gen_range(1..=1000);
        let tx = rng.gen_range(1..=1000000);
        let amount = if ty == "dispute" || ty == "resolve" || ty == "chargeback" {
            "".to_string()
        } else {
            format!("{:.4}", rng.gen_range(0.0..10000.0))
        };
        writeln!(writer, "{},{},{},{}", ty, client, tx, amount)?;
    }
    Ok(())
}

fn process_normal(input_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::default();
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }
    output_results(&engine)
}

fn process_stress_test(
    file_path: &str,
    start_memory: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = Engine::default();
    let start_time = Instant::now();
    match read_and_process_transactions(&mut engine, file_path) {
        Ok(()) => {}
        Err(e) => eprintln!("Error during stress test: {}", e),
    }

    output_results(&engine)?;

    {
        let elapsed_time = start_time.elapsed();
        let memory_delta = get_current_memory(&mut System::new_all()).saturating_sub(start_memory);

        let memory_delta_mb = (memory_delta as f64) / (1024.0 * 1024.0);
        let engine_memory_mb = (engine.size_of() as f64) / (1024.0 * 1024.0);
        eprintln!("Elapsed time: {:?}", elapsed_time);
        eprintln!("Engine Memory size: {:.3} MB", engine_memory_mb);
        eprintln!("Memory consumption delta: {:.3} MB", memory_delta_mb);
    }

    std::fs::remove_file(file_path)?;
    Ok(())
}

fn output_results(engine: &Engine) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = Writer::from_writer(std::io::stdout());
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

fn get_current_memory(system: &mut System) -> u64 {
    system.refresh_all();
    system.processes().values().map(|p| p.memory()).sum()
}
