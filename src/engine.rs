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
    const ERROR_DIFFERENT_CLIENT: &'static str = "Cannot dispute transaction from a different client";
    const ERROR_NO_AMOUNT: &'static str = "Transaction must have an amount";
    const ERROR_DEPOSIT_AMOUNT: &'static str = "Deposit amount must be greater than 0";
    const ERROR_WITHDRAWAL_AMOUNT: &'static str = "Withdrawal amount must be greater than 0";
    const ERROR_TX_REPEATED: &'static str = "Transaction id already processed in this session - cannot be repeated.";
    const ERROR_INSUFFICIENT_FUNDS: &'static str = "Insufficient funds";
    const ERROR_ACCOUNT_NOT_FOUND: &'static str = "Account not found";
    const ERROR_TX_NOT_FOUND: &'static str = "Transaction not found";
    const ERROR_ADDITION_OVERFLOW: &'static str = "Addition overflow";
    const ERROR_SUBTRACTION_OVERFLOW: &'static str = "Subtraction overflow";

    // to manage : 
    const ERROR_ACCOUNT_LOCKED: &'static str = "Account is locked";
    const ERROR_ACCOUNT_NOT_FOUND_IN_TRANSACTION: &'static str = "Account not found in transaction";
    const ERROR_TX_NOT_DISPUTED: &'static str = "Transaction not disputed";
    const ERROR_TX_NOT_RESOLVED: &'static str = "Transaction not resolved";
    

    pub fn new() -> Self {
        Engine {
            accounts: HashMap::new(),
            transaction_log: HashMap::new(),
        }
    }

    fn check_transasction(tx: &Transaction, original_tx: &Transaction) -> Result<Decimal, Box<dyn Error>> {
        if original_tx.client != tx.client {
            return Err(Self::ERROR_DIFFERENT_CLIENT.into());
        }
        let mut amount = original_tx
                    .amount
                    .ok_or(Self::ERROR_NO_AMOUNT)?;
        if original_tx.ty == TransactionType::Withdrawal {
            amount = -amount;
        }
        Ok(amount)
    }

    fn safe_add(a: &Decimal, b: &Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        a.checked_add(*b).ok_or(Self::ERROR_ADDITION_OVERFLOW.into())
    }
    
    fn safe_sub(a: &Decimal, b: &Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        a.checked_sub(*b).ok_or(Self::ERROR_SUBTRACTION_OVERFLOW.into())
    }
}

impl EngineFunctions for Engine {
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

    fn process_deposit(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or(Self::ERROR_NO_AMOUNT)?;
        if amount <= Decimal::from(0) {
            return Err(Self::ERROR_DEPOSIT_AMOUNT.into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(Self::ERROR_TX_REPEATED.into());
        }

        let account = self
            .accounts
            .entry(tx.client)
            .or_insert_with(Account::default);

        account.available =  Engine::safe_add(&account.available,&amount)?;
        account.total =  Engine::safe_add(&account.total,&amount)?;

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }

    fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or(Self::ERROR_NO_AMOUNT)?;
        if amount <= Decimal::from(0) {
            return Err(Self::ERROR_WITHDRAWAL_AMOUNT.into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(Self::ERROR_TX_REPEATED.into());
        }
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.available >= amount {
                account.available =  Engine::safe_sub(&account.available,&amount)?;
                account.total =  Engine::safe_sub(&account.total,&amount)?;
            } else {
                return Err(Self::ERROR_INSUFFICIENT_FUNDS.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }

        self.transaction_log.insert(tx.tx, tx.clone());
        Ok(())
    }
    
    fn process_dispute(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.available =  Engine::safe_sub(&account.available,&amount)?;
                account.held =  Engine::safe_add(&account.held,&amount)?;
            } else {
                return Err(Self::ERROR_TX_NOT_FOUND.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }
        Ok(())
    }

    fn process_resolve(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.available =  Engine::safe_add(&account.available,&amount)?;
                account.held =  Engine::safe_sub(&account.held,&amount)?;
            } else {
                return Err(Self::ERROR_TX_NOT_FOUND.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }
        Ok(())
    }

    fn process_chargeback(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transasction(tx,original_tx)?;
                account.total =  Engine::safe_sub(&account.total,&amount)?;
                account.held =  Engine::safe_sub(&account.held,&amount)?;
                account.locked = true;
            } else {
                return Err(Self::ERROR_TX_NOT_FOUND.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }
        Ok(())
    }
}

const BATCH_SIZE: usize = 16_384;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransactionProcessingError {
    #[error("Multiple errors occurred: {0:?}")]
    MultipleErrors(Vec<String>),
}

pub fn read_and_process_transactions(
    engine: &mut Engine,
    input_path: &str,
) -> Result<(), TransactionProcessingError> {
    let file = std::fs::File::open(input_path).map_err(|e| TransactionProcessingError::MultipleErrors(vec![format!("Error opening file: {}", e)]))?;
    let mut reader = BufReader::with_capacity(BATCH_SIZE, file);

    let mut csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(&mut reader);

    let mut errors = Vec::new();

    loop {
        let mut records = Vec::new();
        for _ in 0..BATCH_SIZE {
            match csv_reader.records().next() {
                Some(Ok(record)) => records.push(record),
                Some(Err(e)) => {
                    errors.push(format!("CSV parsing error: {}", e));
                    continue;
                }
                None => break,
            }
        }

        if records.is_empty() {
            break;
        }

        for record in records {
            let transaction = match Transaction::from_record(&record) {
                Ok(tx) => tx,
                Err(e) => {
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
        Err(TransactionProcessingError::MultipleErrors(errors))
    } else {
        Ok(())
    }
}

