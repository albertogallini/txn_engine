
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::NamedTempFile;
use txn_engine::asyncengine::{AsycEngineFunctions, AsyncEngine};


use std::io::Write;

const BUFFER_SIZE: usize = 16_384;


#[tokio::test]
async fn unit_test_deposit_and_withdrawal_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1, 1, 10.00008,\n
                                withdrawal, 1, 2, 5.0000,\n"#;
    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await
                .expect("CSV processing should succeed");

            // Assertions
            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );
            assert_eq!(
                engine.transaction_log.len().await,
                2,
                "Two transactions processed"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

             assert_eq!(
                account.total,
                Decimal::from_str("5.0001").unwrap(),
                "Total should be 5 after deposit and withdrawal"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("5.0001").unwrap(),
                "Available should match total since no disputes"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
            
            assert!(!account.locked);
        })
    };

    //wait for completion
    handle.await.unwrap();
}



#[tokio::test]
async fn unit_test_deposit_and_dispute_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n"#;
    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await
                .expect("CSV processing should succeed");

            // Assertions
            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );
            assert_eq!(
                engine.transaction_log.len().await,
                1,
                "One transactions processed"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should remain 10 after dispute"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("0.0000").unwrap(),
                "Available should be 0 after dispute"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("10.0000").unwrap(),
                "Held should be 10 after dispute"
            );
            
            assert!(!account.locked);
        })
    };

    //wait for completion
    handle.await.unwrap();
}
