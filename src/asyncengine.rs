#![allow(async_fn_in_trait)]

// src/async_engine.rs
use crate::basics::hmap::ShardedRwLockMap; // your new map
use crate::datastr::account::Account;
use crate::datastr::transaction::{
    ClientId, Transaction, TransactionProcessingError, TransactionType, TxId,
};
use csv::ReaderBuilder;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use csv_async::{AsyncReaderBuilder, AsyncWriterBuilder, Trim};
use futures_util::stream::StreamExt;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, RwLockWriteGuard};
use tokio::task;
use tokio_util::io::SyncIoBridge;

// Reuse the same errors
pub use crate::engine::{EngineError, EngineSerDeserError};

#[derive(Debug, Error)]
pub enum AsycEngineSerDeserError {
    #[error("I/O error while reading session")]
    Io(std::io::Error),
    #[error("Parsing error while reading session csv")]
    Csv(csv_async::Error),
    #[error("Parsing error while reading session csv - InvalidClientId")]
    InvalidClientId,
    #[error("Parsing error while reading session csv - InvalidDecimal")]
    InvalidDecimal,
    #[error("Parsing error while reading session csv - InvalidBool")]
    InvalidBool,
}

pub trait AsycEngineFunctions {
    async fn read_and_process_transactions<R: AsyncRead + Unpin + Send + 'static>(
        &self, // self is NOT mutable as this function can be called concurrently and its implementation must be thread-safe.
        stream: R,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError>;
    async fn read_and_process_transactions_from_csv(
        &self,
        input_path: &str,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError>;
    async fn load_from_previous_session_csvs(
        &self,
        transactions_file: &str,
        accounts_file: &str,
    ) -> Result<(), AsycEngineSerDeserError>;
    async fn dump_account_to_csv<W: AsyncWriteExt + Unpin + AsyncWrite>(
        &self,
        writer: W,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn dump_transaction_log_to_csv(
        &self,
        transactions_path: &str,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn size_of(&self) -> usize;
}

trait AsycEngineStateTransitionFunctions {
    async fn process_transaction(&self, tx: &Transaction) -> Result<(), EngineError>;
    async fn process_deposit(&self, tx: &Transaction) -> Result<(), EngineError>;
    async fn process_withdrawal(&self, tx: &Transaction) -> Result<(), EngineError>;
    async fn process_dispute(&self, tx: &Transaction) -> Result<(), EngineError>;
    async fn process_resolve(&self, tx: &Transaction) -> Result<(), EngineError>;
    async fn process_chargeback(&self, tx: &Transaction) -> Result<(), EngineError>;
}

#[derive(Default)]
pub struct AsyncEngine {
    pub accounts: Arc<ShardedRwLockMap<ClientId, Account>>,
    pub transaction_log: Arc<ShardedRwLockMap<TxId, Transaction>>,
}

impl AsyncEngine {
    pub fn new() -> Self {
        Self {
            accounts: Arc::new(ShardedRwLockMap::new()),
            transaction_log: Arc::new(ShardedRwLockMap::new()),
        }
    }

    // Helper: safe math (same as sync version)
    fn safe_add(a: Decimal, b: Decimal) -> Result<Decimal, EngineError> {
        a.checked_add(b).ok_or(EngineError::AdditionOverflow)
    }

    fn safe_sub(a: Decimal, b: Decimal) -> Result<Decimal, EngineError> {
        a.checked_sub(b).ok_or(EngineError::SubtractionOverflow)
    }

    async fn try_get_account(
        &self,
        client: ClientId,
    ) -> Result<RwLockWriteGuard<'_, HashMap<ClientId, Account>>, EngineError> {
        self.accounts
            .get_mut(client)
            .await
            .ok_or(EngineError::AccountNotFound)
            .and_then(|guard| {
                if (*guard).get(&client).unwrap().locked {
                    Err(EngineError::AccountLocked)
                } else {
                    Ok(guard)
                }
            })
    }

    fn check_transaction_semantic(
        tx: &Transaction,
        original_tx: &Transaction,
    ) -> Result<Decimal, EngineError> {
        // Identical to your sync version
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
        let mut amount = original_tx
            .amount
            .ok_or(EngineError::ReferredTransactionNoAmount)?;
        if original_tx.ty == TransactionType::Withdrawal {
            amount = -amount;
        }
        Ok(amount)
    }
}

impl AsycEngineFunctions for AsyncEngine {
    async fn read_and_process_transactions<R>(
        &self,
        stream: R,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        // Channel to parallelize CSV reading (producer) and transaction processing (consumer)
        let (tx_sender, mut tx_receiver) = mpsc::unbounded_channel::<Transaction>();
        let (err_sender, mut err_receiver) = mpsc::unbounded_channel::<String>();

        let handle = task::spawn_blocking(move || {
            // We need to wrap the async stream in a SyncIoBridge to convert it to a sync stream
            // because the csv library only supports sync streams.
            // We then create a BufReader with the specified buffer size to efficiently read the stream.
            // Finally, we create a CSV reader from the BufReader.
            let sync_stream = SyncIoBridge::new(stream);
            let mut reader = std::io::BufReader::with_capacity(buffer_size, sync_stream);
            let mut csv_reader = ReaderBuilder::new()
                .has_headers(true)
                .trim(csv::Trim::All)
                .from_reader(&mut reader);

            for result in csv_reader.deserialize::<Transaction>() {
                match result {
                    Ok(tx) => {
                        if tx_sender.send(tx).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let msg = if e.to_string().contains("unknown variant") {
                            "Error reading transaction record: unknown transaction type".to_string()
                        } else {
                            format!("Error reading transaction record: {}", e)
                        };
                        let _ = err_sender.send(msg);
                    }
                }
            }
        });
        let mut errors = Vec::new();

        while let Some(tx) = tx_receiver.recv().await {
            if let Err(e) = self.process_transaction(&tx).await {
                errors.push(format!("Error processing {tx:?}: {e}"));
            }
        }

        while let Ok(err) = err_receiver.try_recv() {
            errors.push(err);
        }

        if handle.await.is_err() {
            errors.push("CSV parser panicked".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TransactionProcessingError::MultipleErrors(errors))
        }
    }

    async fn read_and_process_transactions_from_csv(
        &self, // note: &self, not &mut self – we only write to thread-safe structures
        input_path: &str,
        buffer_size: usize,
    ) -> Result<(), TransactionProcessingError> {
        let file = File::open(input_path).await.map_err(|e| {
            TransactionProcessingError::MultipleErrors(vec![format!("Error opening file: {}", e)])
        })?;

        let reader = BufReader::with_capacity(buffer_size, file);
        self.read_and_process_transactions(reader, buffer_size)
            .await
    }

    async fn load_from_previous_session_csvs(
        &self,
        transactions_file: &str,
        accounts_file: &str,
    ) -> Result<(), AsycEngineSerDeserError> {
        // Load transactions
        {
            let file = File::open(transactions_file)
                .await
                .map_err(AsycEngineSerDeserError::Io)?;
            let mut rdr = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::All)
                .create_deserializer(BufReader::new(file));

            let mut records = rdr.deserialize::<Transaction>();
            while let Some(result) = records.next().await {
                if let Ok(tx) = result {
                    self.transaction_log.insert(tx.tx, tx).await;
                } else {
                    eprintln!("Skipping invalid transaction record: {:?}", result);
                }
            }
        }

        // Load accounts (custom format: client,available,held,total,locked)
        {
            let file = File::open(accounts_file)
                .await
                .map_err(AsycEngineSerDeserError::Io)?;

            let mut reader = AsyncReaderBuilder::new()
                .has_headers(true)
                .trim(Trim::All)
                .create_deserializer(BufReader::new(file));

            type AccountTuple = (ClientId, String, String, String, bool);

            let mut records = reader.deserialize::<AccountTuple>();

            while let Some(result) = records.next().await {
                let (client_id, available_str, held_str, total_str, locked) =
                    result.map_err(AsycEngineSerDeserError::Csv)?;

                let to_dec = |s: String| -> Result<Decimal, _> {
                    s.parse::<Decimal>()
                        .map_err(|_| AsycEngineSerDeserError::InvalidDecimal)
                };

                let available = to_dec(available_str)?;
                let held = to_dec(held_str)?;
                let total = to_dec(total_str)?;

                let account = Account {
                    available,
                    held,
                    total,
                    locked,
                };

                self.accounts.insert(client_id, account).await;
            }
        }

        Ok(())
    }

    async fn dump_account_to_csv<W: AsyncWrite + Unpin + AsyncWrite>(
        &self,
        writer: W,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut csv_writer = AsyncWriterBuilder::new()
            .buffer_capacity(buffer_size)
            .create_serializer(writer);

        // Write header
        csv_writer
            .serialize(("client", "available", "held", "total", "locked"))
            .await?;

        let mut iter = self.accounts.iter().await;
        while let Some((client_id, shard_guard)) = iter.next().await {
            // Safety: we know key exists in this shard
            if let Some(account) = shard_guard.get(&client_id) {
                csv_writer
                    .serialize((
                        client_id,
                        account.available,
                        account.held,
                        account.total,
                        account.locked,
                    ))
                    .await?;

                //flush every N records to reduce memory
                if client_id % 1000 == 0 {
                    csv_writer.flush().await?;
                }
            }
        }

        // Write header + all buffered data in one go
        csv_writer.flush().await?;

        Ok(())
    }

    async fn dump_transaction_log_to_csv(
        &self,
        transactions_path: &str,
        buffer_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(transactions_path).await?;
        let buffered_file = BufWriter::with_capacity(buffer_size, file);

        let mut csv_writer = AsyncWriterBuilder::new()
            .buffer_capacity(buffer_size)
            .create_serializer(buffered_file); // ← works directly!

        // Write header
        csv_writer
            .serialize(("type", "client", "tx", "amount", "disputed"))
            .await?;

        let mut iter = self.transaction_log.iter().await;
        while let Some((tx_id, shard_guard)) = iter.next().await {
            if let Some(transaction) = shard_guard.get(&tx_id) {
                // Write a record to the CSV file
                csv_writer
                    .serialize((
                        transaction.ty.clone(),
                        transaction.client,
                        transaction.tx,
                        transaction.amount,
                        transaction.disputed,
                    ))
                    .await?;
            }
        }
        csv_writer.flush().await?;
        Ok(())
    }

    async fn size_of(&self) -> usize {
        let accounts_size = self.accounts.len().await
            * (std::mem::size_of::<ClientId>() + std::mem::size_of::<Account>());

        let tx_log_size = self.transaction_log.len().await
            * (std::mem::size_of::<TxId>() + std::mem::size_of::<Transaction>());

        std::mem::size_of::<Self>() + accounts_size + tx_log_size
    }
}

impl AsycEngineStateTransitionFunctions for AsyncEngine {
    async fn process_transaction(&self, tx: &Transaction) -> Result<(), EngineError> {
        match tx.ty {
            TransactionType::Deposit => self.process_deposit(tx).await,
            TransactionType::Withdrawal => self.process_withdrawal(tx).await,
            TransactionType::Dispute => self.process_dispute(tx).await,
            TransactionType::Resolve => self.process_resolve(tx).await,
            TransactionType::Chargeback => self.process_chargeback(tx).await,
        }
    }

    async fn process_deposit(&self, tx: &Transaction) -> Result<(), EngineError> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::ZERO {
            return Err(EngineError::DepositAmountInvalid);
        }
        if self.transaction_log.contains_key(tx.tx).await {
            return Err(EngineError::TransactionRepeated);
        }

        let mut account_guard = self.accounts.entry(tx.client).await;
        let account = account_guard.get_mut(&tx.client).unwrap();

        if account.locked {
            return Err(EngineError::AccountLocked);
        }

        account.available = Self::safe_add(account.available, amount)?;
        account.total = Self::safe_add(account.total, amount)?;

        self.transaction_log.insert(tx.tx, tx.clone()).await;
        Ok(())
    }

    async fn process_withdrawal(&self, tx: &Transaction) -> Result<(), EngineError> {
        let amount = tx.amount.ok_or(EngineError::NoAmount)?;
        if amount <= Decimal::ZERO {
            return Err(EngineError::WithdrawalAmountInvalid);
        }
        if self.transaction_log.contains_key(tx.tx).await {
            return Err(EngineError::TransactionRepeated);
        }

        let mut account_guard = self.try_get_account(tx.client).await?;
        let account = account_guard.get_mut(&tx.client).unwrap();

        if account.available >= amount {
            account.available = Self::safe_sub(account.available, amount)?;
            account.total = Self::safe_sub(account.total, amount)?;
        } else {
            return Err(EngineError::InsufficientFunds);
        }

        self.transaction_log.insert(tx.tx, tx.clone()).await;
        Ok(())
    }

    async fn process_dispute(&self, tx: &Transaction) -> Result<(), EngineError> {
        let mut account_guard = self.try_get_account(tx.client).await?;
        let account = account_guard.get_mut(&tx.client).unwrap();

        if let Some(mut original_tx_guard) = self.transaction_log.get_mut(tx.tx).await {
            let original_tx = original_tx_guard.get_mut(&tx.tx).unwrap();
            let amount = Self::check_transaction_semantic(tx, original_tx)?;
            account.available = Self::safe_sub(account.available, amount)?;
            account.held = Self::safe_add(account.held, amount)?;
            original_tx.disputed = true;
        } else {
            return Err(EngineError::TransactionNotFound);
        }
        Ok(())
    }

    async fn process_resolve(&self, tx: &Transaction) -> Result<(), EngineError> {
        let mut account_guard = self.try_get_account(tx.client).await?;
        let account = account_guard.get_mut(&tx.client).unwrap();

        if let Some(mut original_tx_guard) = self.transaction_log.get_mut(tx.tx).await {
            let original_tx = original_tx_guard.get_mut(&tx.tx).unwrap();
            let amount = Self::check_transaction_semantic(tx, original_tx)?;
            account.available = Self::safe_add(account.available, amount)?;
            account.held = Self::safe_sub(account.held, amount)?;
            original_tx.disputed = false;
        } else {
            return Err(EngineError::TransactionNotFound);
        }
        Ok(())
    }

    async fn process_chargeback(&self, tx: &Transaction) -> Result<(), EngineError> {
        let mut account_guard = self.try_get_account(tx.client).await?;
        let account = account_guard.get_mut(&tx.client).unwrap();

        if let Some(original_tx_guard) = self.transaction_log.get(tx.tx).await {
            let original_tx = original_tx_guard.get(&tx.tx).unwrap();
            let amount = Self::check_transaction_semantic(tx, original_tx)?;
            account.total = Self::safe_sub(account.total, amount)?;
            account.held = Self::safe_sub(account.held, amount)?;
            account.locked = true;
        } else {
            return Err(EngineError::TransactionNotFound);
        }
        Ok(())
    }
}
