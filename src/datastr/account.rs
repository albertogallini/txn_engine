use csv::Writer;
use rust_decimal::Decimal;
use std::collections::HashMap;

// Represents an account
#[derive(Debug, Default, Clone)]
pub struct Account {
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

/// Writes the account balances to the standard output in CSV format.
///
/// # Parameters
/// - `accounts`: A reference to a `HashMap` where the key is a `u16` representing the client ID,
///   and the value is an `Account` containing the balance details.
///
/// # Returns
/// - `Ok(())`: If the account balances are successfully written.
/// - `Err(Box<dyn std::error::Error>)`: If there is an error during writing.
pub fn write_account_balances(
    accounts: &HashMap<u16, Account>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = Writer::from_writer(std::io::stdout());

    for (client, account) in accounts {
        writer.write_record(&[
            client.to_string(),
            account.available.to_string(),
            account.held.to_string(),
            account.total.to_string(),
            account.locked.to_string(),
        ])?;
    }

    writer.flush()?;
    Ok(())
}
