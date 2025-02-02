use crate::datastr::account::Account;
use crate::datastr::transaction::{ClientId, Transaction, TransactionProcessingError, TransactionType, TxId};
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
    const ERROR_ACCOUNT_LOCKED: &'static str = "Account is locked";
    const ERROR_TX_ALREADY_DISPUTED: &'static str = "Transaction already disputed"; 
    const ERROR_TX_NOT_DISPUTED: &'static str = "Transaction not disputed";
    

    pub fn new() -> Self {
        Engine {
            accounts: HashMap::new(),
            transaction_log: HashMap::new(),
        }
    }

    /// Verifies the semantic validity of a transaction in relation to its original transaction.
    ///
    /// # Parameters
    /// - `tx`: The transaction to be checked.
    /// - `original_tx`: The original transaction that `tx` is related to.
    ///
    /// # Returns
    /// - `Ok(Decimal)`: The amount associated with the original transaction, possibly negated if
    ///   the original transaction was a withdrawal.
    /// - `Err(Box<dyn Error>)`: An error if the transactions have different clients, the transaction
    ///   type requires a disputed status that doesn't match, or if the original transaction lacks an amount.
    ///
    /// # Errors
    /// - `ERROR_DIFFERENT_CLIENT`: If the transactions are from different clients.
    /// - `ERROR_TX_ALREADY_DISPUTED`: If a dispute is attempted on an already disputed transaction.
    /// - `ERROR_TX_NOT_DISPUTED`: If a resolve or chargeback is attempted on a non-disputed transaction.
    /// - `ERROR_NO_AMOUNT`: If the original transaction does not have an amount.

    fn check_transaction_semantic(tx: &Transaction, original_tx: &Transaction) -> Result<Decimal, Box<dyn Error>> {
        if original_tx.client != tx.client {
            return Err(Self::ERROR_DIFFERENT_CLIENT.into());
        }
        match tx.ty  {
            TransactionType::Dispute => {
                if original_tx.disputed {
                    return Err(Self::ERROR_TX_ALREADY_DISPUTED.into());
                }
            }
            TransactionType::Resolve => {
                if !original_tx.disputed {
                    return Err(Self::ERROR_TX_NOT_DISPUTED.into());
                }
            }
            TransactionType::Chargeback => {
                if !original_tx.disputed {
                    return Err(Self::ERROR_TX_NOT_DISPUTED.into());
                }
            }
            _ => {}
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
    
    /// Process a transaction. This function is a dispatch to the correct processing function
    /// for the given transaction type.
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
    /// # Parameters
    /// - `tx`: The deposit transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the transaction is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid or if the account is locked.
    ///
    /// # Errors
    /// - `ERROR_NO_AMOUNT`: If the transaction does not have an amount.
    /// - `ERROR_DEPOSIT_AMOUNT`: If the transaction amount is not greater than 0.
    /// - `ERROR_TX_REPEATED`: If the transaction id has already been processed in this session.
    /// - `ERROR_ACCOUNT_LOCKED`: If the account is already locked.
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

        if account.locked {
            return Err(Self::ERROR_ACCOUNT_LOCKED.into());
        }

        account.available =  Engine::safe_add(&account.available,&amount)?;
        account.total =  Engine::safe_add(&account.total,&amount)?;

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
    /// - `Err(Box<dyn Error>)`: If the transaction is invalid, if the account is locked,
    ///   if there are insufficient funds, or if the account is not found.
    ///
    /// # Errors
    /// - `ERROR_NO_AMOUNT`: If the transaction does not have an amount.
    /// - `ERROR_WITHDRAWAL_AMOUNT`: If the transaction amount is not greater than 0.
    /// - `ERROR_TX_REPEATED`: If the transaction id has already been processed in this session.
    /// - `ERROR_ACCOUNT_LOCKED`: If the account is already locked.
    /// - `ERROR_INSUFFICIENT_FUNDS`: If the account has insufficient available funds.
    /// - `ERROR_ACCOUNT_NOT_FOUND`: If the account does not exist.

    fn process_withdrawal(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        let amount = tx.amount.ok_or(Self::ERROR_NO_AMOUNT)?;
        if amount <= Decimal::from(0) {
            return Err(Self::ERROR_WITHDRAWAL_AMOUNT.into());
        }
        if self.transaction_log.contains_key(&tx.tx) {
            return Err(Self::ERROR_TX_REPEATED.into());
        }
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(Self::ERROR_ACCOUNT_LOCKED.into());
            }
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
    
    /// Processes a dispute transaction by moving the disputed amount from the available balance
    /// to the held balance and marking the transaction as disputed.
    ///
    /// # Parameters
    /// - `tx`: The dispute transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the dispute is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the account is locked, if the account or transaction is not found,
    ///   or if the transaction semantics are invalid.
    ///
    /// # Errors
    /// - `ERROR_ACCOUNT_LOCKED`: If the account is locked.
    /// - `ERROR_ACCOUNT_NOT_FOUND`: If the account does not exist.
    /// - `ERROR_TX_NOT_FOUND`: If the original transaction is not found.
    /// - Additional errors from `check_transaction_semantic` for semantic validation.

    fn process_dispute(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(Self::ERROR_ACCOUNT_LOCKED.into());
            }
            if let Some(original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx,original_tx)?;
                account.available =  Engine::safe_sub(&account.available,&amount)?;
                account.held =  Engine::safe_add(&account.held,&amount)?;
                original_tx.disputed = true;
            } else {
                return Err(Self::ERROR_TX_NOT_FOUND.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }
        Ok(())
    }

    /// Processes a resolve transaction by moving the resolved amount from the held balance
    /// back to the available balance and marking the transaction as not disputed.
    ///
    /// # Parameters
    /// - `tx`: The resolve transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the resolve is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the account is locked, if the account or transaction is not found,
    ///   or if the transaction semantics are invalid.
    ///
    /// # Errors
    /// - `ERROR_ACCOUNT_LOCKED`: If the account is locked.
    /// - `ERROR_ACCOUNT_NOT_FOUND`: If the account does not exist.
    /// - `ERROR_TX_NOT_FOUND`: If the original transaction is not found.
    /// - Additional errors from `check_transaction_semantic` for semantic validation.

    fn process_resolve(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(Self::ERROR_ACCOUNT_LOCKED.into());
            }
            if let Some(original_tx) = self.transaction_log.get_mut(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx,original_tx)?;
                account.available =  Engine::safe_add(&account.available,&amount)?;
                account.held =  Engine::safe_sub(&account.held,&amount)?;
                original_tx.disputed = false;
            } else {
                return Err(Self::ERROR_TX_NOT_FOUND.into());
            }
        } else {
            return Err(Self::ERROR_ACCOUNT_NOT_FOUND.into());
        }
        Ok(())
    }

    /// Processes a chargeback transaction by moving the chargeback amount from the held balance
    /// and locking the account.
    ///
    /// # Parameters
    /// - `tx`: The chargeback transaction to be processed.
    ///
    /// # Returns
    /// - `Ok(())`: If the chargeback is successfully processed.
    /// - `Err(Box<dyn Error>)`: If the account is locked, if the account or transaction is not found,
    ///   or if the transaction semantics are invalid.
    ///
    /// # Errors
    /// - `ERROR_ACCOUNT_LOCKED`: If the account is locked.
    /// - `ERROR_ACCOUNT_NOT_FOUND`: If the account does not exist.
    /// - `ERROR_TX_NOT_FOUND`: If the original transaction is not found.
    /// - Additional errors from `check_transaction_semantic` for semantic validation.

    fn process_chargeback(&mut self, tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if let Some(account) = self.accounts.get_mut(&tx.client) {
            if account.locked {
                return Err(Self::ERROR_ACCOUNT_LOCKED.into());
            }
            if let Some(original_tx) = self.transaction_log.get(&tx.tx) {
                let amount = Engine::check_transaction_semantic(tx,original_tx)?;
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
            match csv_reader.deserialize::<Transaction>().next() {
                Some(Ok(record)) => records.push(record),
                Some(Err(e)) => {
                    errors.push(format!("Error reading transaction record: {}", e));
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
                errors.push(format!(
                    "Error processing {:?}: {}",
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
