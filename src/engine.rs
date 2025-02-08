use crate::datastr::account::{serialize_account_balances_csv, Account};
use crate::datastr::transaction::{
    serialize_transcation_log_csv, ClientId, Transaction, TransactionProcessingError,
    TransactionType, TxId,
};
use dashmap::DashMap;
use std::fs::File;
use thiserror::Error;

use csv::{ReaderBuilder, Trim};
use rust_decimal::Decimal;
use std::io::{BufReader, BufWriter, Read, Write};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Cannot dispute/resolve/chargeback transaction from a different client")]
    DifferentClient,
    #[error("Transaction must have an amount")]
    NoAmount,
    #[error("Referred Transaction must have an amount")]
    ReferredTransactionNoAmount,
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

#[derive(Debug, Error)]
pub enum EngineSerDeserError {
    #[error("I/O error while reading session")]
    Io(std::io::Error),
    #[error("Parsing error while reading session csv")]
    Csv(csv::Error),
    #[error("Parsing error while reading session csv - InvalidClientId")]
    InvalidClientId,
    #[error("Parsing error while reading session csv - InvalidDecimal")]
    InvalidDecimal,
    #[error("Parsing error while reading session csv - InvalidBool")]
    InvalidBool,
}

impl From<std::io::Error> for EngineSerDeserError {
    fn from(err: std::io::Error) -> Self {
        EngineSerDeserError::Io(err)
    }
}

impl From<csv::Error> for EngineSerDeserError {
    fn from(err: csv::Error) -> Self {
        EngineSerDeserError::Csv(err)
    }
}

pub trait EngineFunctions {
    fn read_and_process_transactions<R: Read>(
        &self, // self is NOT mutable as this function can be called concurrently and its implementation must be thread-safe.
        stream: R,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError>;
    fn read_and_process_transactions_from_csv(
        &mut self,
        input_path: &str,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError>;
    fn load_from_previous_session_csvs(
        &mut self,
        transactions_file: &str,
        accounts_file: &str,
    ) -> Result<(), EngineSerDeserError>;
    fn dump_account_to_csv<W: Write>(
        &self,
        writer: W,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn dump_transaction_log_to_csv(
        &self,
        transactions_path: &str,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>>;
    fn size_of(&self) -> usize;
}

trait EngineStateTransitionFunctions {
    fn process_transaction(&self, tx: &Transaction) -> Result<(), EngineError>;
    fn process_deposit(&self, tx: &Transaction) -> Result<(), EngineError>;
    fn process_withdrawal(&self, tx: &Transaction) -> Result<(), EngineError>;
    fn process_dispute(&self, tx: &Transaction) -> Result<(), EngineError>;
    fn process_resolve(&self, tx: &Transaction) -> Result<(), EngineError>;
    fn process_chargeback(&self, tx: &Transaction) -> Result<(), EngineError>;
}

#[derive(Default)]
pub struct Engine {
    pub accounts: DashMap<ClientId, Account>,
    pub transaction_log: DashMap<TxId, Transaction>,
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
    /// - `ReferredTransactionNoAmount`: If the original transaction does not have an amount.
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
        // This error condition should never happen as it is guaranteed only deposits and withdrawals
        // with valid amount are stored in the transactions_log. But in general the process could read
        // the content from a previous session from file if the file is corrupted some deposits or
        // withdrawals without amount could occur.
        let mut amount = original_tx
            .amount
            .ok_or(EngineError::ReferredTransactionNoAmount)?;

        if original_tx.ty == TransactionType::Withdrawal {
            amount = -amount;
        }
        Ok(amount)
    }

    /// Performs a safe addition of two decimal numbers.
    ///
    /// This function is used to add two decimal numbers together without overflowing.
    /// If the addition would overflow, it returns an error.
    ///
    /// # Parameters
    /// - `a`: The first decimal number to add.
    /// - `b`: The second decimal number to add.
    ///
    /// # Returns
    /// - `Ok(Decimal)`: The result of the addition if it doesn't overflow.
    /// - `Err(EngineError)`: An error if the addition overflows.
    ///
    /// # Errors
    /// - `AdditionOverflow`: If the addition overflows.
    fn safe_add(a: &Decimal, b: &Decimal) -> Result<Decimal, EngineError> {
        a.checked_add(*b).ok_or(EngineError::AdditionOverflow)
    }

    /// Performs a safe subtraction of two decimal numbers.
    ///
    /// This function is used to subtract two decimal numbers without underflowing.
    /// If the subtraction would underflow, it returns an error.
    ///
    /// # Parameters
    /// - `a`: The first decimal number to subtract from.
    /// - `b`: The second decimal number to subtract.
    ///
    /// # Returns
    /// - `Ok(Decimal)`: The result of the subtraction if it doesn't underflow.
    /// - `Err(EngineError)`: An error if the subtraction underflows.
    ///
    /// # Errors
    /// - `SubtractionOverflow`: If the subtraction underflows.
    fn safe_sub(a: &Decimal, b: &Decimal) -> Result<Decimal, EngineError> {
        a.checked_sub(*b).ok_or(EngineError::SubtractionOverflow)
    }

    /// Reads transactions from a stream in chunk and processes them.
    ///
    /// This method is designed to handle large inputs by reading in chunks,
    /// allowing for control over memory usage based on the provided batch size.
    /// NOTE: referce to self is not mutable as this specific implementation only change dashmap which are thread-safe
    /// and it is safe to call this functions form multiple threads concurrently.
    ///
    /// # Parameters
    /// - `stream`: Any type that implements `Read`, providing the transaction data.
    /// - `buffer_size`: # of bytes in each chunk.
    ///
    /// # Returns
    /// - `Ok(())` if all transactions are processed without errors.
    /// - `Err(TransactionProcessingError)` if any errors occur during processing or reading.
    fn read_and_process_transactions<R: Read>(
        &self,
        stream: R,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError> {
        let reader = BufReader::with_capacity(buffer_size, stream);

        let mut csv_reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

        let mut errors = Vec::new();

        loop {
            let mut records = Vec::new();
            for _ in 0..buffer_size {
                match csv_reader.deserialize::<Transaction>().next() {
                    Some(Ok(record)) => records.push(record),
                    Some(Err(e)) => {
                        let error_message = e.to_string();
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
                if let Err(e) = self.process_transaction(&transaction) {
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
}

impl EngineFunctions for Engine {
    /// Estimates the memory size of the `Engine` including all its data structures.
    ///
    /// This method provides an APPROXIMATE size in bytes since it can't account for
    /// all memory overheads like those in hashmaps or other complex data structures.
    ///
    /// # Returns
    /// - `usize`: The estimated size in bytes.
    fn size_of(&self) -> usize {
        let mut size = std::mem::size_of_val(self);

        size += self.accounts.len()
            * (std::mem::size_of::<ClientId>() + std::mem::size_of::<Account>());

        size += self.transaction_log.len()
            * (std::mem::size_of::<TxId>() + std::mem::size_of::<Transaction>());

        size
    }

    /// Reads transactions from an input stream and processes them using the Engine.
    ///
    ///
    /// # Parameters
    /// - `stream`: Any type that implements `Read`, providing the stream to read from.
    ///    It is NOT mutable as this function is thread-safe.
    /// - `buffer_size`: The size of the internal buffer used to read from the stream.
    ///
    /// # Returns
    /// - `Result<(), TransactionProcessingError>`: Returns `Ok(())` if all transactions
    ///   are processed successfully, or `Err(TransactionProcessingError)` if any
    ///   errors occur during processing.
    fn read_and_process_transactions<R: Read>(
        &self,
        stream: R,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError> {
        self.read_and_process_transactions(stream, buffer_size)
    }

    /// Reads transactions from a CSV file and processes them using the Engine.
    ///
    /// # Parameters
    /// - `input_path`: The path to the CSV file containing the transactions.
    /// - `buffer_size`: The number of records to buffer while processing to ensure memory efficiency.
    ///
    /// # Returns
    /// - `Result<(), TransactionProcessingError>`: `Ok(())` if the transactions are processed successfully, `Err(TransactionProcessingError)` if errors occur while processing.
    fn read_and_process_transactions_from_csv(
        &mut self,
        input_path: &str,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError> {
        let file = File::open(input_path).map_err(|e| {
            TransactionProcessingError::MultipleErrors(vec![format!("Error opening file: {}", e)])
        })?;
        let reader = BufReader::new(file);

        // Call the method from the Engine struct
        self.read_and_process_transactions(reader, buffer_size)
    }

    /// Loads transactions and accounts from CSV files dumped from a previous session to populate the internal maps.
    ///
    /// This method reads from two CSV files: one for transactions and one for accounts.
    /// The CSV file for accounts includes the client ID as the first field, which is not part
    /// of the `Account` structure itself but used as a key in `DashMap`.
    ///
    /// NOTE: This function is very naive and does NOT perform any semantic/consistency check on the input data
    ///       so bad or inconsistent account/transaction_log can be effectively created.
    ///       !!  -- This function is meant to be used only on verified input files.  -- !!
    ///
    /// # Parameters
    /// - `transactions_path`: Path to the CSV file containing transactions.
    /// - `accounts_path`: Path to the CSV file containing account details.
    ///
    /// # Returns
    /// - `Result<(), EngineError>`: Ok if loading was successful, or an error if there were issues with file reading or parsing.
    fn load_from_previous_session_csvs(
        &mut self,
        transactions_path: &str,
        accounts_path: &str,
    ) -> Result<(), EngineSerDeserError> {
        // Load transactions from CSV
        {
            let file = File::open(transactions_path).map_err(EngineSerDeserError::Io)?;
            let mut rdr = ReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::All)
                .from_reader(BufReader::new(file));

            for result in rdr.deserialize::<Transaction>() {
                match result {
                    Ok(transaction) => {
                        self.transaction_log.insert(transaction.tx, transaction);
                    }
                    Err(e) => {
                        eprintln!("Error parsing CSV record: {:?}", e);
                        match e.kind() {
                            csv::ErrorKind::Deserialize {
                                pos: Some(pos),
                                err,
                            } => eprintln!(
                                "Error at line {}, position {}: {}",
                                pos.line(),
                                pos.byte(),
                                err
                            ),
                            _ => eprintln!("Unexpected CSV error: {}", e),
                        }
                    }
                }
            }
        }

        // Load accounts from CSV
        {
            let file = File::open(accounts_path).map_err(EngineSerDeserError::Io)?;
            let mut rdr = ReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::All)
                .from_reader(BufReader::new(file));

            for result in rdr.records() {
                let record = result.map_err(EngineSerDeserError::Csv)?;
                let client_id: u16 = record[0]
                    .parse()
                    .map_err(|_| EngineSerDeserError::InvalidClientId)?;
                let account = Account {
                    available: record[1]
                        .parse()
                        .map_err(|_| EngineSerDeserError::InvalidDecimal)?,
                    held: record[2]
                        .parse()
                        .map_err(|_| EngineSerDeserError::InvalidDecimal)?,
                    total: record[3]
                        .parse()
                        .map_err(|_| EngineSerDeserError::InvalidDecimal)?,
                    locked: record[4]
                        .parse()
                        .map_err(|_| EngineSerDeserError::InvalidBool)?,
                };
                self.accounts.insert(client_id, account);
            }
        }

        Ok(())
    }

    /// Dumps the current state of all accounts to a CSV writer.
    ///
    /// The CSV writer is wrapped with a `BufWriter` to improve performance and the memory usage.
    /// The `buffer_size` parameter specifies the size of the buffer to use.
    ///
    /// The first line of the CSV file is the header row, containing the column names:
    /// `client`, `available`, `held`, `total`, and `locked`.
    ///
    /// # Errors
    /// - `Box<dyn std::error::Error>` if any errors occur while writing to the writer.
    fn dump_account_to_csv<W: Write>(
        &self,
        writer: W,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Wrap the writer with a buffered writer

        let mut buf_writer = BufWriter::with_capacity(buffer_size, writer);
        writeln!(buf_writer, "client,available,held,total,locked")?;
        buf_writer.flush()?; // Ensure the header is written

        serialize_account_balances_csv(&self.accounts, &mut buf_writer)?;

        buf_writer.flush()?;

        Ok(())
    }

    /// Dumps the current state of all transactions to a CSV file.
    ///
    /// The first line of the CSV file is the header row, containing the column names:
    /// `type`, `client`, `tx`, `amount`, and `disputed`.
    ///
    /// # Parameters
    /// - `transactions_path`: Path to the CSV file to write to.
    /// - `buffer_size`: Size of the buffer to use for writing to the file.
    ///
    /// # Errors
    /// - `Box<dyn std::error::Error>` if any errors occur while writing to the file.
    fn dump_transaction_log_to_csv(
        &self,
        transactions_path: &str,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Dump transactions

        let file = File::create(transactions_path)?;
        let mut buf_writer = BufWriter::with_capacity(buffer_size, file);

        writeln!(buf_writer, "type,client,tx,amount,disputed")?;
        buf_writer.flush()?; // Ensure the header is written

        serialize_transcation_log_csv(&self.transaction_log, &mut buf_writer)?;

        buf_writer.flush()?;

        Ok(())
    }
}

impl EngineStateTransitionFunctions for Engine {
    /// Process a transaction. This function is a dispatch to the correct processing function
    /// for the given transaction type.
    fn process_transaction(&self, tx: &Transaction) -> Result<(), EngineError> {
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
    /// - `Err(EngineError)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `NoAmount`: If the transaction does not have an amount.
    /// - `DepositAmountInvalid`: If the transaction amount is not greater than 0.
    /// - `TransactionRepeated`: If the transaction id has already been processed in this session.
    /// - `AccountLocked`: If the account is already locked.
    fn process_deposit(&self, tx: &Transaction) -> Result<(), EngineError> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::from(0) {
            return Err(EngineError::DepositAmountInvalid);
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(EngineError::TransactionRepeated);
        }

        let mut account = self.accounts.entry(tx.client).or_default();

        if account.locked {
            return Err(EngineError::AccountLocked);
        }

        account.available = Engine::safe_add(&account.available, &amount)?;
        account.total = Engine::safe_add(&account.total, &amount)?;

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }

    /// Process a withdrawal transaction.
    ///
    /// # Parameters
    /// - `tx`: The withdrawal transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(EngineError)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `NoAmount`: If the transaction does not have an amount.
    /// - `WithdrawalAmountInvalid`: If the transaction amount is not greater than 0.
    /// - `TransactionRepeated`: If the transaction id has already been processed in this session.
    /// - `AccountLocked`: If the account is already locked.
    /// - `InsufficientFunds`: If the account does not have enough available funds.
    /// - `AccountNotFound`: If the account does not exist.
    fn process_withdrawal(&self, tx: &Transaction) -> Result<(), EngineError> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::from(0) {
            return Err(EngineError::WithdrawalAmountInvalid);
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(EngineError::TransactionRepeated);
        }
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked);
            }
            if account.available >= amount {
                account.available = Engine::safe_sub(&account.available, &amount)?;
                account.total = Engine::safe_sub(&account.total, &amount)?;
            } else {
                return Err(EngineError::InsufficientFunds);
            }
        } else {
            return Err(EngineError::AccountNotFound);
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
    /// - `Err(EngineError)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_dispute(&self, tx: &Transaction) -> Result<(), EngineError> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked);
            }
            if let Some(mut original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.available = Engine::safe_sub(&account.available, &amount)?;
                account.held = Engine::safe_add(&account.held, &amount)?;
                original_tx.disputed = true;
            } else {
                return Err(EngineError::TransactionNotFound);
            }
        } else {
            return Err(EngineError::AccountNotFound);
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
    /// - `Err(EngineError)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_resolve(&self, tx: &Transaction) -> Result<(), EngineError> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked);
            }
            if let Some(mut original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.available = Engine::safe_add(&account.available, &amount)?;
                account.held = Engine::safe_sub(&account.held, &amount)?;
                original_tx.disputed = false;
            } else {
                return Err(EngineError::TransactionNotFound);
            }
        } else {
            return Err(EngineError::AccountNotFound);
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
    /// - `Err(EngineError)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `TransactionNotFound`: If the transaction id is not found in the transaction log.
    /// - `AccountNotFound`: If the client id is not found in the accounts map.
    /// - `AccountLocked`: If the account is already locked.
    fn process_chargeback(&self, tx: &Transaction) -> Result<(), EngineError> {
        if let Some(mut account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(EngineError::AccountLocked);
            }
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx, &original_tx)?;
                account.total = Engine::safe_sub(&account.total, &amount)?;
                account.held = Engine::safe_sub(&account.held, &amount)?;
                account.locked = true;
            } else {
                return Err(EngineError::TransactionNotFound);
            }
        } else {
            return Err(EngineError::AccountNotFound);
        }

        Ok(())
    }
}
