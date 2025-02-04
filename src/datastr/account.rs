use csv::Writer;
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write;

use super::deser::{deserialize_amount_r, deserialize_trimmed_string};

// Represents an account
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
pub struct Account {
    #[serde(deserialize_with = "deserialize_amount_r")]
    pub available: Decimal,
    #[serde(deserialize_with = "deserialize_amount_r")]
    pub held: Decimal,
    #[serde(deserialize_with = "deserialize_amount_r")]
    pub total: Decimal,
    #[serde(deserialize_with = "deserialize_trimmed_string::<bool,_>")]
    pub locked: bool,
}

/// Writes the final state of all accounts to stdout as a CSV file.
///
/// This function is used at the end of the `txn_engine` to output the final state of all accounts to stdout.
/// The order of the columns is:
/// - client: The client ID.
/// - available: The available balance for the client.
/// - held: The held balance for the client.
/// - total: The total balance for the client.
/// - locked: Whether the account is locked.
///
/// # Errors
/// - `Box<dyn std::error::Error>` if any errors occur while writing to stdout.
pub fn serialize_account_balances_csv<W: Write>(
    accounts: &DashMap<u16, Account>,
    writer: W,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_writer = Writer::from_writer(writer);

    for entry in accounts.iter() {
        let client_id = *entry.key();
        let account = entry.value();

        // Write a record to the CSV file
        csv_writer.serialize((
            client_id,
            account.available,
            account.held,
            account.total,
            account.locked,
        ))?;
    }
    csv_writer.flush()?;
    Ok(())
}
