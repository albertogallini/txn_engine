use crate::datastr::account::Account;
use crate::datastr::transaction::{
    ClientId, Transaction, TransactionProcessingError, TransactionType, TxId,
};
use dashmap::DashMap;
use std::error::Error;
use thiserror::Error;

use csv::ReaderBuilder;
use rust_decimal::Decimal;
use std::io::BufReader;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Cannot dispute transaction from a different client")]
    DifferentClient,
    #[error("Transaction must have an amount")]
    NoAmount,
    #[error("Deposit amount must be greater than 0")]
    DepositAmountInvalid,
    #[error("Withdrawal amount must be greater than 0")]
    WithdrawalAmountInvalid,
    #[error("Transaction id already processed in this session - cannot be repeated.")]
    TransactionRepeated,
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Account not found")]
    AccountNotFound,
    #[error("Transaction not found")]
    TransactionNotFound,
    #[error("Addition overflow")]
    AdditionOverflow,
    #[error("Subtraction overflow")]
    SubtractionOverflow,
    #[error("Account is locked")]
    AccountLocked,
    #[error("Transaction already disputed")]
    TransactionAlreadyDisputed,
    #[error("Transaction not disputed")]
    TransactionNotDisputed,
}

#[derive(Default)]
pub struct Engine {
    pub accounts: DashMap<ClientId, Account>,
    pub transaction_log: DashMap<TxId, Transaction>,
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
            accounts: DashMap::new(),
            transaction_log: DashMap::new(),
        }
    }

    /// Verifies the semantic validity of a transaction in relation to its original transaction.
    ///
    /// # Parameters
    /// - `tx`: The transaction to be checked.
    /// - `original_tx`: The original transaction that `tx` is related to.
    ///
    /// # Returns
    /// - `Ok(Decimal)`: The amount associated with the original transaction, POSSIBLY WITH A NEGATIVE SIGN if
    ///   the original transaction was a withdrawal.
    /// - `Err(EngineError)`: An error if the transactions have different clients, the transaction
    ///   type requires a disputed status that doesn't match, or if the original transaction lacks an amount.
    ///
    /// # Errors
    /// - `DifferentClient`: If the transactions are from different clients.
    /// - `TransactionAlreadyDisputed`: If a dispute is attempted on an already disputed transaction.
    /// - `TransactionNotDisputed`: If a resolve or chargeback is attempted on a non-disputed transaction.
    /// - `NoAmount`: If the original transaction does not have an amount.
    fn check_transaction_semantic(
        tx: &Transaction,
        original_tx: &Transaction,
    ) -> Result<Decimal, EngineError> {
        if original_tx.client != tx.client {
            return Err(EngineError::DifferentClient);
        }
        match tx.ty {
            TransactionType::Dispute => {
                if original_tx.disputed {
                    return Err(EngineError::TransactionAlreadyDisputed);
                }
            }
            TransactionType::Resolve | TransactionType::Chargeback => {
                if !original_tx.disputed {
                    return Err(EngineError::TransactionNotDisputed);
                }
            }
            _ => {}
        }
        let mut amount = original_tx.amount.ok_or(EngineError::NoAmount)?;

        if original_tx.ty == TransactionType::Withdrawal {
            amount = -amount;
        }
        Ok(amount)
    }

    fn safe_add(a: &Decimal, b: &Decimal) -> Result<Decimal, EngineError> {
        a.checked_add(*b).ok_or(EngineError::AdditionOverflow)
    }

    fn safe_sub(a: &Decimal, b: &Decimal) -> Result<Decimal, EngineError> {
        a.checked_sub(*b).ok_or(EngineError::SubtractionOverflow)
    }

    /// Estimates the memory size of the `Engine` including all its data structures.
    ///
    /// This method provides an APPROXIMATE size in bytes since it can't account for
    /// all memory overheads like those in hashmaps or other complex data structures.
    ///
    /// # Returns
    /// - `usize`: The estimated size in bytes.
    pub fn size_of(&self) -> usize {
        let mut size = std::mem::size_of_val(self);

        size += self.accounts.len()
            * (std::mem::size_of::<ClientId>() + std::mem::size_of::<Account>());

        size += self.transaction_log.len()
            * (std::mem::size_of::<TxId>() + std::mem::size_of::<Transaction>());

        size
    }
}

impl EngineFunctions for Engine {
    /// Process a transaction. This function is a dispatch to the correct processing function
    /// for the given transaction type.
    fn process_transaction(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        match tx.ty {
            TransactionType::Deposit => self.process_deposit(tx)?,
            TransactionType::Withdrawal => self.process_withdrawal(tx)?,
            TransactionType::Dispute => self.process_dispute(tx)?,
            TransactionType::Resolve => self.process_resolve(tx)?,
            TransactionType::Chargeback => self.process_chargeback(tx)?,
        }
        Ok(())
    }

    /// Process a deposit transaction.
    ///
    /// # Parameters
    /// - `tx`: The deposit transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `NoAmount`: If the transaction does not have an amount.
    /// - `DepositAmountInvalid`: If the transaction amount is not greater than 0.
    /// - `TransactionRepeated`: If the transaction id has already been processed in this session.
    /// - `AccountLocked`: If the account is already locked.
    fn process_deposit(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::from(0) {
            return Err(EngineError::DepositAmountInvalid.into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(EngineError::TransactionRepeated.into());
        }

        let mut account = self.accounts.entry(tx.client).or_default();

        if account.locked {
            return Err(EngineError::AccountLocked.into());
        }

        account.available = Engine::safe_add(&account.available, &amount)?;
        account.total = Engine::safe_add(&account.total, &amount)?;

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }

    // ... Similar adjustments for process_withdrawal, process_dispute, process_resolve, and process_chargeback:

    fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::from(0) {
            return Err(EngineError::WithdrawalAmountInvalid.into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(EngineError::TransactionRepeated.into());
        }
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked.into());
            }
            if account.available >= amount {
                account.available = Engine::safe_sub(&account.available, &amount)?;
                account.total = Engine::safe_sub(&account.total, &amount)?;
            } else {
                return Err(EngineError::InsufficientFunds.into());
            }
        } else {
            return Err(EngineError::AccountNotFound.into());
        }

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }

    /// Process a dispute transaction.
    ///
    /// # Parameters
    /// - `tx`: The dispute transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_dispute(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked.into());
            }
            if let Some(mut original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.available = Engine::safe_sub(&account.available, &amount)?;
                account.held = Engine::safe_add(&account.held, &amount)?;
                original_tx.disputed = true;
            } else {
                return Err(EngineError::TransactionNotFound.into());
            }
        } else {
            return Err(EngineError::AccountNotFound.into());
        }
        Ok(())
    }

    /// Process a resolve transaction.
    ///
    /// # Parameters
    /// - `tx`: The resolve transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_resolve(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked.into());
            }
            if let Some(mut original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.available = Engine::safe_add(&account.available, &amount)?;
                account.held = Engine::safe_sub(&account.held, &amount)?;
                original_tx.disputed = false;
            } else {
                return Err(EngineError::TransactionNotFound.into());
            }
        } else {
            return Err(EngineError::AccountNotFound.into());
        }
        Ok(())
    }

    /// Process a chargeback transaction.
    ///
    /// # Parameters
    /// - `tx`: The chargeback transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_chargeback(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked.into());
            }
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.total = Engine::safe_sub(&account.total, &amount)?;
                account.held = Engine::safe_sub(&account.held, &amount)?;
                account.locked = true;
            } else {
                return Err(EngineError::TransactionNotFound.into());
            }
        } else {
            return Err(EngineError::AccountNotFound.into());
        }

        Ok(())
    }
}

const BATCH_SIZE: usize = 16_384;

/// Reads the given CSV file and processes each transaction with the given engine.
///
/// The CSV file is expected to have the following format:
///
/// - The first row is expected to be a header row with the columns "client", "tx", "type", and "amount".
/// - Each row after the header row is expected to represent a transaction, with the columns
///   "client", "tx", "type", and "amount" representing the client id, transaction id, transaction type,
///   and amount of the transaction, respectively.
///
/// # Errors
/// - `TransactionProcessingError`: If any errors occur while reading the CSV file or processing the transactions.
///   The error will contain a list of all errors that occurred.
/// - `std::io::Error`: If an I/O error occurs while reading the CSV file.
///
/// The BATCH_SIZE constant controls how many records are read from the CSV file at a time before being
/// processed. A larger BATCH_SIZE can improve performance by reducing the number of times the CSV file
/// needs to be read from disk, but it also increases memory usage. A smaller BATCH_SIZE can reduce memory
/// usage at the cost of slower performance. The  value is set to 16_384, but it can be expsoed as process
/// parameter.
///
pub fn read_and_process_transactions(
    engine: &mut Engine,
    input_path: &str,
) -> Result<(), TransactionProcessingError> {
    let file = std::fs::File::open(input_path).map_err(|e| {
        TransactionProcessingError::MultipleErrors(vec![format!("Error opening file: {}", e)])
    })?;
    let mut reader = BufReader::with_capacity(BATCH_SIZE, file);

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(&mut reader);

    let mut errors = Vec::new();

    loop {
        let mut records = Vec::new();
        for _ in 0..BATCH_SIZE {
            match csv_reader.deserialize::<Transaction>().next() {
                Some(Ok(record)) => records.push(record),
                Some(Err(e)) => {
                    let error_message = e.to_string();
                    // Do not want to print the whole error message, just the part coming from the
                    // Transaction deserializer, if possible.
                    if let Some(pos) = error_message.find("Unknown transaction type") {
                        errors.push(format!(
                            "Error reading transaction record: {}",
                            &error_message[pos..]
                        ));
                    } else {
                        errors.push(format!("Error reading transaction record: {}", e));
                    }
                    continue;
                }
                None => break,
            }
        }

        if records.is_empty() {
            break;
        }

        for transaction in records {
            if let Err(e) = engine.process_transaction(&transaction) {
                errors.push(format!("Error processing {:?}: {}", transaction, e));
            }
        }
    }

    if !errors.is_empty() {
        Err(TransactionProcessingError::MultipleErrors(errors))
    } else {
        Ok(())
    }
}
