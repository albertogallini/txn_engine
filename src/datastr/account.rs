use crate::basics::hmap::ShardedRwLockMap;
use csv::Writer;
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::io::Write;

use super::{
    deser::{deserialize_account_amount, deserialize_trimmed_string},
    transaction::ClientId,
};

// Represents an account
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
pub struct Account {
    #[serde(deserialize_with = "deserialize_account_amount")]
    pub available: Decimal,
    #[serde(deserialize_with = "deserialize_account_amount")]
    pub held: Decimal,
    #[serde(deserialize_with = "deserialize_account_amount")]
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
    accounts: &DashMap<ClientId, Account>,
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

use csv::WriterBuilder;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// Writes the final state of all accounts to the given writer as a CSV file.
///
/// This function is used at the end of the `txn_engine` to output the final state of all accounts to a writer.
/// The order of the columns is:
/// - client: The client ID.
/// - available: The available balance for the client.
/// - held: The held balance for the client.
/// - total: The total balance for the client.
/// - locked: Whether the account is locked.
///
/// # Errors
/// - `Box<dyn std::error::Error + Send + Sync>` if any errors occur while writing to the writer.
pub async fn serialize_account_balances_csv_async<W>(
    accounts: &ShardedRwLockMap<ClientId, Account>,
    mut writer: W,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    W: AsyncWrite + Unpin + Send + Sync,
{
    // Use csv::Writer with a Vec<u8> buffer first (fastest path)
    let mut buffer = Vec::with_capacity(8192);
    {
        let mut csv_writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut buffer);

        let mut iter = accounts.iter().await;
        while let Some((client_id, shard_guard)) = iter.next().await {
            // Safety: we know key exists in this shard
            if let Some(account) = shard_guard.get(&client_id) {
                csv_writer.serialize((
                    client_id,
                    account.available,
                    account.held,
                    account.total,
                    account.locked,
                ))?;

                //flush every N records to reduce memory
                if client_id % 1000 == 0 {
                    csv_writer.flush()?;
                    writer.flush().await?;
                }
            }
        }
    }

    // Write header + all buffered data in one go
    writer
        .write_all(b"client,available,held,total,locked\n")
        .await?;
    writer.write_all(&buffer).await?;
    writer.flush().await?;

    Ok(())
}
