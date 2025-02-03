use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer};
use std::fmt;

use super::deser::{deserialize_amount, deserialize_trimmed_string};

pub type TxId = u32;
pub type ClientId = u16;

#[derive(PartialEq, Debug, Clone)]
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

/*

Reasons to use serde:
CSV to Struct: when reading from CSV, you might want to directly convert each row into a Transaction struct.
Serde can automatically map CSV fields to struct fields if you use the #[derive(Deserialize)] attribute on your structs.

Struct to CSV: when writing back to CSV, Serde can serialize your structs back into CSV format,
ensuring that data integrity is maintained without manual string formatting.

Consistent Data Handling: using Serde ensures that data is consistently formatted when both reading
 from and writing to files, which reduces errors in data representation.

Extensibility: if you later decide to store or transmit data in a different format
(like JSON for API responses, or binary formats for efficiency), Serde can handle these conversions
without changing your core data structures. This makes your code more adaptable to changes in data storage or transmission methods.

 */

#[derive(Deserialize, Debug, Clone)]
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

impl<'de> Deserialize<'de> for TransactionType {
    /// Deserializes a `TransactionType` from a string representation.
    ///
    /// # Parameters
    /// - `deserializer`: The deserializer to read the string from.
    ///
    /// # Returns
    /// - `Ok(TransactionType)`: The corresponding `TransactionType` if the string matches
    ///   a known transaction type.
    /// - `Err(D::Error)`: If the string does not match any known transaction type.
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
