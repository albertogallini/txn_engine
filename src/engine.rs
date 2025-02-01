use crate::datastr::account::Account;
use crate::datastr::transaction::{ClientId, Transaction, TransactionType, TxId};
use std::collections::HashMap;
use std::error::Error;

use csv::ReaderBuilder;
use rust_decimal::Decimal;
use std::io::BufReader;

#[derive(Default)]
pub struct Engine {
    pub accounts: HashMap<ClientId, Account>,
    pub transaction_log: HashMap<TxId, Transaction>,
}

pub trait EngineFunctions {
    fn process_transaction(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
    fn process_deposit(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
    fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
    fn process_dispute(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
    fn process_resolve(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
    fn process_chargeback(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>>;
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            accounts: HashMap::new(),
            transaction_log: HashMap::new(),
        }
    }

    /// Checks that a transaction is valid to dispute or resolve.
    ///
    /// - Verifies that the client ID matches between the two transactions.
    /// - Verifies that the original transaction has an amount.
    /// - If the original transaction is a withdrawal, reverses the amount.
    ///
    /// # Errors
    ///
    /// - Returns an error if the client IDs do not match.
    /// - Returns an error if the original transaction does not have an amount.
    ///
    fn check_transasction(tx: &Transaction, original_tx: &Transaction) -> Result<Decimal, Box<dyn Error>> {
        if original_tx.client != tx.client {
            return Err("Cannot dispute transaction from a different client".into());
        }
        let mut amount = original_tx
                    .amount
                    .ok_or("Disputed transaction must have an amount")?;
        if original_tx.ty == TransactionType::Withdrawal { // Reverse the transaction amount if it's a withdrawal
            amount = -amount;
        }

        Ok(amount)
    }

    fn safe_add(a: &Decimal, b: &Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        a.checked_add(*b).ok_or("Addition overflowed".into())
    }
    
    // Similarly for other operations like:
    fn safe_sub(a: &Decimal, b: &Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        a.checked_sub(*b).ok_or("Subtraction overflowed".into())
    }

}

impl EngineFunctions for Engine {

    
    /// Process a transaction. This function is a thin wrapper around the
    /// different transaction processors (deposit, withdrawal, dispute, resolve,
    /// chargeback). It takes a transaction and applies the correct rules for
    /// that transaction type to the accounts.
    fn process_transaction(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        match tx.ty {
            TransactionType::Deposit => self.process_deposit(tx)?,
            TransactionType::Withdrawal => self.process_withdrawal(tx)?,
            TransactionType::Dispute => self.process_dispute(tx)?,
            TransactionType::Resolve => self.process_resolve(tx)?,
            TransactionType::Chargeback => self.process_chargeback(tx)?
        }
        Ok(())
    }

    /// Process a deposit transaction.
    ///
    /// - Creates an account if it does not exist
    /// - Increases the available and total balances of the account by the amount
    /// - Adds the transaction to the transaction log
    ///
    /// # Errors
    ///
    /// - Returns an error if the transaction does not have an amount.
    fn process_deposit(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or("Deposit must have an amount")?;
        if amount <= Decimal::from(0) {
            return Err("Deposit amount must be greater than 0".into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err("Transaction id already processed in this session - cannot be repeated.".into());
        }

        // If the account doesn't exist, create it
        let account = self
            .accounts
            .entry(tx.client)
            .or_insert_with(Account::default);

        account.available =  Engine::safe_add(&account.available,&amount)?;
        account.total =  Engine::safe_add(&account.total,&amount)?;

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }

    /// Process a withdrawal transaction.
    ///
    /// - Decreases the available and total balances of the account by the amount.
    /// - Adds the transaction to the transaction log if successful.
    ///
    /// # Errors
    ///
    /// - Returns an error if the transaction does not have an amount.
    /// - Returns an error if the account is not found.
    /// - Returns an error if the available balance is insufficient.

    fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or("Withdrawal must have an amount")?;
        if amount <= Decimal::from(0) {
            return Err("Withdrawal amount must be greater than 0".into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err("Transaction id already processed in this session - cannot be repeated.".into());
        }
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.available >= amount {
                account.available =  Engine::safe_sub(&account.available,&amount)?;
                account.total =  Engine::safe_sub(&account.total,&amount)?;
            } else {
                return Err("Insufficient funds".into());
            }
        } else {
            return Err("Account not found".into());
        }

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }
    

    /// Process a dispute transaction.
    ///
    /// - Decreases the available balance of the account by the amount of the original transaction.
    /// - Increases the held balance of the account by the amount of the original transaction.
    /// - check the referred transasction.
    ///
    /// # Errors
    ///
    /// - Returns an error if the client IDs do not match.
    /// - Returns an error if the original transaction does not have an amount.
    /// - Returns an error if the account is not found.
    /// - Returns an error if the original transaction is not found.
    fn process_dispute(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.available =  Engine::safe_sub(&account.available,&amount)?;
                account.held =  Engine::safe_add(&account.held,&amount)?;
                
            } else {
                return Err("Transaction not found".into());
            }
        } else {
            return Err("Account not found".into());
        }
        Ok(())
    }

    /// Process a resolve transaction.
    ///
    /// - Increases the available balance of the account by the amount of the original transaction.
    /// - Decreases the held balance of the account by the amount of the original transaction.
    /// - check the referred transasction.
    ///
    /// # Errors
    ///
    /// - Returns an error if the client IDs do not match.
    /// - Returns an error if the original transaction does not have an amount.
    /// - Returns an error if the account is not found.
    /// - Returns an error if the original transaction is not found.

    fn process_resolve(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.available =  Engine::safe_add(&account.available,&amount)?;
                account.held =  Engine::safe_sub(&account.held,&amount)?;
            } else {
                return Err("Transaction not found".into());
            }
        } else {
            return Err("Account not found".into());
        }
        Ok(())
    }

    /// Process a chargeback transaction.
    ///
    /// - Decreases the held balance of the account by the amount of the original transaction.
    /// - Decreases the total balance of the account by the amount of the original transaction.
    /// - Locks the account, preventing further transactions.
    /// - check the referred transasction.
    ///
    /// # Errors
    ///
    /// - Returns an error if the client IDs do not match.
    /// - Returns an error if the original transaction does not have an amount.
    /// - Returns an error if the account is not found.
    /// - Returns an error if the original transaction is not found.

    fn process_chargeback(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.total =  Engine::safe_sub(&account.total,&amount)?;
                account.held =  Engine::safe_sub(&account.held,&amount)?;
                account.locked = true;
                
            } else {
                return Err("Transaction not found".into());
            }
        } else {
            return Err("Account not found".into());
        }
        Ok(())
    }
}

// Batch size:
const BATCH_SIZE: usize = 16_384; // 128 x 128 bytes
                                  /*  128 transactions per batch (assuming the avg size of a transaction is 128 bytes) is a good compromise between memory usage and processing speed.
                                      But this should be an imput parameter to dynamically adjust the behaviour of the function:

                                      - Small Batch Size:
                                      Pro:
                                       Lower memory usage since you're only holding a small amount of data in memory at any one time.
                                       Better for systems with constrained memory or when dealing with streams where you don't know the total data size upfront.
                                      Cons:
                                       Increased I/O Operations: You'll perform more reads from the disk or network, which can slow down your application due to the latency of each I/O operation.
                                       Disk I/O, in particular, can be a bottleneck if you're constantly seeking and reading small chunks of data.

                                      - Large Batch Size or One Big Read:
                                      Pro:
                                       Reduced I/O Operations: Fewer, larger reads mean less time spent waiting for I/O operations.
                                       This can significantly improve performance, especially on traditional spinning disks where seek times are high.
                                       Even on SSDs or when dealing with network streams, fewer reads can still benefit by reducing the overhead of initiating reads.
                                      Cons:
                                       Higher Memory Usage: You need enough memory to hold all the data you're reading in one go or in larger chunks.
                                       If the dataset is too large, you risk running out of memory or causing the system to swap memory to disk, which can negate the I/O benefits.
                                       Performance Hit for Very Large Datasets: If you load everything into memory at once,
                                       you might encounter performance issues due to memory constraints or if the dataset exceeds available RAM, leading to swapping.

                                  */

/// Reads a CSV file and processes it in batches of BATCH_SIZE transactions.
///
/// The provided closure `process_batch` is called for each batch of transactions.
/// It receives a slice of `Transaction`s and should return a `Result` to indicate
/// whether the processing was successful.
///
/// The function returns an error if the file cannot be opened or if an error
/// occurs while reading the file.
///
/// NOTE: the CSV file is expected to have the header "ty,client,tx,amount".
///
pub fn read_and_process_transactions(
    engine: &mut Engine,
    input_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(input_path)?;
    let mut reader = BufReader::with_capacity(BATCH_SIZE, file); // Adjust buffer size

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true) // Assuming the first row isn't headers
        .from_reader(&mut reader);

    let mut errors = Vec::new(); // To collect errors from transactions

    loop {
        let mut records = Vec::new();
        for _ in 0..BATCH_SIZE {
            match csv_reader.records().next() {
                Some(Ok(record)) => records.push(record),
                Some(Err(e)) => {
                    errors.push(format!("CSV parsing error: {}", e));
                    continue;
                }
                None => break, // No more records
            }
        }

        // debug -- println!("Processing batch of #records {:?}", records.len());
        if records.is_empty() {
            break; // End of file
        }

        for record in records {
            // debug -- println!("Processing record: {:?}", record);
            let transaction = match Transaction::from_record(&record) {
                Ok(tx) => tx,
                Err(e) => {
                    print!("record: {:?}---\n", record);
                    errors.push(format!("Error creating transaction from record: {}", e));
                    continue;
                }
            };

            if let Err(e) = engine.process_transaction(&transaction) {
                errors.push(format!(
                    "Error processing transaction {:?}: {}",
                    transaction, e
                ));
            }
        }
    }

    if !errors.is_empty() {
        for error in &errors {
            eprintln!("{}", error);
        }
        Err("Encountered errors during transaction processing".into())
    } else {
        Ok(())
    }
}
