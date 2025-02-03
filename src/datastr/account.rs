use csv::Writer;
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::deser::{deserialize_amount_r, deserialize_trimmed_string};

// Represents an account
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
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

/// Writes the account balances to the standard output in CSV format using DashMap.
///
/// This function uses deserialization methods for formatting output, ensuring
/// consistency in how data is presented. It iterates directly over the `DashMap`
/// without cloning the accounts, for low memory usage.
///
/// # Parameters
/// - `accounts`: A reference to a `DashMap` where the key is a `u16` representing the client ID,
///   and the value is an `Account` containing the balance details.
///
/// # Returns
/// - `Ok(())`: If the account balances are successfully written.
/// - `Err(Box<dyn std::error::Error>)`: If there is an error during writing.
pub fn write_account_balances(
    accounts: &DashMap<u16, Account>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = Writer::from_writer(std::io::stdout());

    for r in accounts.iter() {
        let (client, account) = r.pair();
        // Use deserialization methods indirectly for formatting
        let available_str = format!("{}", account.available);
        let held_str = format!("{}", account.held);
        let total_str = format!("{}", account.total);
        let locked_str = if account.locked { "true" } else { "false" };

        writer.write_record(&[
            client.to_string(),
            available_str,
            held_str,
            total_str,
            locked_str.to_owned(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}
