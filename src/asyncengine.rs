// src/async_engine.rs
use crate::basics::hmap::ShardedRwLockMap; // your new map
use crate::datastr::account::{serialize_account_balances_csv_async, Account};
use crate::datastr::transaction::{
    ClientId, Transaction, TransactionProcessingError, TransactionType, TxId,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::RwLockWriteGuard;

// Reuse the same errors
pub use crate::engine::{EngineError, EngineSerDeserError};

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

impl AsyncEngine {
    pub async fn process_transaction(&self, tx: &Transaction) -> Result<(), EngineError> {
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

    pub async fn read_and_process_transactions_from_csv(
        &self,
        path: &str,
    ) -> Result<(), TransactionProcessingError> {
        let file = File::open(path).await.map_err(|e| {
            TransactionProcessingError::MultipleErrors(vec![format!("Failed to open file: {}", e)])
        })?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // Skip header
        let _ = lines.next_line().await;

        let mut errors = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            match csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(line.as_bytes())
                .deserialize::<Transaction>()
                .next()
            {
                Some(Ok(tx)) => {
                    if let Err(e) = self.process_transaction(&tx).await {
                        errors.push(format!("tx {}: {}", tx.tx, e));
                    }
                }
                Some(Err(e)) => errors.push(format!("CSV error: {}", e)),
                None => continue,
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(TransactionProcessingError::MultipleErrors(errors))
        }
    }

    pub async fn dump_accounts_to_csv(
        &self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let file = File::create(path).await?;
        let mut writer = BufWriter::new(file);
        writer
            .write_all(b"client,available,held,total,locked\n")
            .await?;
        serialize_account_balances_csv_async(&self.accounts, &mut writer).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn size_of(&self) -> usize {
        let accounts = self.accounts.len().await;
        let txns = self.transaction_log.len().await;
        accounts * 80 + txns * 120 // rough estimate
    }
}
