use csv::StringRecord;
use rust_decimal::Decimal;
use std::{fmt, str::FromStr};

pub type TxId = u32;
pub type ClientId = u16;

#[derive(PartialEq,Debug,Clone)]
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

impl FromStr for TransactionType {
    type Err = String;

    /// Converts a string to a `TransactionType`. The string is case-insensitive
    /// and can be any of the following:
    ///
    /// - "deposit"
    /// - "withdrawal"
    /// - "dispute"
    /// - "resolve"
    /// - "chargeback"
    ///
    /// If the string is not any of the above, an error is returned.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deposit" => Ok(TransactionType::Deposit),
            "withdrawal" => Ok(TransactionType::Withdrawal),
            "dispute" => Ok(TransactionType::Dispute),
            "resolve" => Ok(TransactionType::Resolve),
            "chargeback" => Ok(TransactionType::Chargeback),
            _ => Err(format!("Unknown transaction type: {}", s)),
        }
    }
}

// Represents a transaction
#[derive(Debug, Clone)]
pub struct Transaction {
    pub ty: TransactionType,
    pub client: ClientId,
    pub tx: TxId,
    pub amount: Option<Decimal>,
    pub disputed: bool,
}

impl Transaction {
    /// Creates a `Transaction` from a `StringRecord` read from a CSV file.
    ///
    /// # Parameters
    /// - `record`: A `StringRecord` read from a CSV file.
    ///
    /// # Returns
    /// - `Ok(Transaction)`: If the record is successfully parsed.
    /// - `Err(Box<dyn Error>)`: If the record is invalid or if the transaction type is unknown.
    ///
    /// # Errors
    /// - `ERROR_UNKNOWN_TRANSACTION_TYPE`: If the transaction type is unknown.
    pub fn from_record(record: &StringRecord) -> Result<Self, Box<dyn std::error::Error>> {
        let ty = TransactionType::from_str(&record[0].trim())?;
        let client = record[1].parse()?;
        let tx = record[2].parse()?;
        let amount = if record[3].trim().is_empty() {
            None
        } else {
            let mut decimal = Decimal::from_str(record[3].trim())?;
            // Round to 4 decimal places
            decimal = decimal.round_dp(4);
            Some(decimal)
        };

        Ok(Transaction {
            ty,
            client,
            tx,
            amount,
            disputed: false,
        })
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
