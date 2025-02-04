use csv::Writer;
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use std::{fmt, io::Write};

use super::deser::{deserialize_amount, deserialize_trimmed_string};

pub type TxId = u32;
pub type ClientId = u16;

#[derive(PartialEq, Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")] // This will convert enum variant names to lowercase for serialization
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl fmt::Display for TransactionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionType::Deposit => write!(f, "deposit"),
            TransactionType::Withdrawal => write!(f, "withdrawal"),
            TransactionType::Dispute => write!(f, "dispute"),
            TransactionType::Resolve => write!(f, "resolve"),
            TransactionType::Chargeback => write!(f, "chargeback"),
        }
    }
}

// Custom Deserialize implementation for TransactionType
impl<'de> Deserialize<'de> for TransactionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str().trim() {
            "deposit" => Ok(TransactionType::Deposit),
            "withdrawal" => Ok(TransactionType::Withdrawal),
            "dispute" => Ok(TransactionType::Dispute),
            "resolve" => Ok(TransactionType::Resolve),
            "chargeback" => Ok(TransactionType::Chargeback),
            _ => Err(serde::de::Error::custom(format!(
                "Unknown transaction type: {}",
                s
            ))),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub ty: TransactionType,
    #[serde(deserialize_with = "deserialize_trimmed_string::<u16,_>")]
    pub client: u16,
    #[serde(deserialize_with = "deserialize_trimmed_string::<u32,_>")]
    pub tx: u32,
    #[serde(deserialize_with = "deserialize_amount")]
    pub amount: Option<Decimal>,
    #[serde(default)]
    pub disputed: bool,
}

#[derive(Debug)]
pub enum TransactionProcessingError {
    MultipleErrors(Vec<String>),
}

impl fmt::Display for TransactionProcessingError {
    /// Formats a `TransactionProcessingError` as a string.
    ///
    /// # Parameters
    /// - `f`: The `Formatter` to write to.
    ///
    /// # Returns
    /// - `Ok(())`: If the error is successfully formatted.
    /// - `Err(fmt::Error)`: If the formatting fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionProcessingError::MultipleErrors(errors) => {
                writeln!(f, "Some errors occurred while processing transactions:")?;
                for error in errors {
                    writeln!(f, "  - {}", error)?;
                }
                Ok(())
            }
        }
    }
}

/// Writes the transaction log to a CSV file.
///
/// The order of the columns is:
/// - ty: The type of the transaction.
/// - client: The client ID.
/// - tx: The transaction ID.
/// - amount: The amount of the transaction.
/// - disputed: Whether the transaction is disputed.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while writing to the CSV file.
pub fn serialize_transcation_log_csv<W: Write>(
    transaction_log: &DashMap<TxId, Transaction>,
    writer: W,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_writer = Writer::from_writer(writer);

    for entry in transaction_log.iter() {
        let transaction = entry.value();

        // Write a record to the CSV file
        csv_writer.serialize((
            transaction.ty.clone(),
            transaction.client,
            transaction.tx,
            transaction.amount,
            transaction.disputed,
        ))?;
    }
    csv_writer.flush()?;
    Ok(())
}
