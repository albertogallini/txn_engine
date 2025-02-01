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
    pub fn from_record(record: &StringRecord) -> Result<Self, Box<dyn std::error::Error>> {
        let ty = TransactionType::from_str(&record[0].trim())?;
        Ok(Transaction {
            ty,
            client: record[1].parse()?,
            tx: record[2].parse()?,
            amount: if record[3].trim().is_empty() {
                None
            } else {
                Some(Decimal::from_str(record[3].trim())?)
            },
            disputed: false,
        })
    }
}


#[derive(Debug)]
pub enum TransactionProcessingError {
    MultipleErrors(Vec<String>),
}

impl fmt::Display for TransactionProcessingError {
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
