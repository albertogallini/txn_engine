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

#[tokio::test]
async fn unit_test_dispute_deposit_after_withdrawal_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                withdrawal,1,3,20.0000,\n;
                                dispute,1,2,,\n"#;

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

            assert_eq!(
                engine.accounts.len().await,
                1,
                "Account should exist even if zero balance"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should be 10 after failed withdrawal"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("-10.0000").unwrap(),
                "Available should be -10 after failed withdrawal and dispute"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("20.0000").unwrap(),
                "Held should be 20 after dispute"
            );

            assert!(!account.locked);
        })
    };
    //wait for completion
    handle.await.unwrap();
}


#[tokio::test]
async fn unit_test_double_dispute_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                withdrawal,1,3,5.0000,\n;
                                dispute,1,3,,\n
                                dispute,1,2,,\n"#;

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

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("25.0000").unwrap(),
                "Total should remain 25 after dispute"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should be 10 after dispute"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("15.0000").unwrap(),
                "Held should be 15 after dispute"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_txid_reused_after_dispute_and_resolve_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                dispute,1,2,,\n#
                                resolve,1,2,,\n
                                deposit,1,2,20.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            let result = engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("{ ty: Deposit, client: 1, tx: 2, amount: Some(20.0000), disputed: false }: Transaction id already processed in this session - cannot be repeated"),
                "Expected `ty: Deposit, client: 1, tx: 2, amount: Some(20.0000), disputed: false : Transaction id already processed in this session - cannot be repeated` error"
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "Account should exist even if zero balance"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("30.0000").unwrap(),
                "Total should be 30"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("30.0000").unwrap(),
                "Available should be 30"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_deposit_and_dispute_resolve_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n
                                resolve,1,1,,\n"#;

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

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
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
                Decimal::from_str("10.0000").unwrap(),
                "Available should be 10 after resolve"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0 after resolve"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_deposit_and_dispute_chargeback_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n
                                chargeback,1,1,,\n"#;

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

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("0.0000").unwrap(),
                "Total should be 0 after chargeback"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("0.0000").unwrap(),
                "Available should be 0 after chargeback"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0 after chargeback"
            );
            assert!(account.locked, "Account must be locked after chargeback");
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_deposit_withdrawal_dispute_withdrawal_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,5.0000,\n
                                dispute,1,2,,\n"#;

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

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("5.0000").unwrap(),
                "Total should be 5 after transactions"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should reflect disputed withdrawal"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("-5.0000").unwrap(),
                "Held should be -5 for disputed withdrawal"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_deposit_withdrawal_too_much_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            let result = engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Insufficient funds"),
                "Expected `Insufficient funds` error"
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should remain 10 after failed withdrawal"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should remain 10 after failed withdrawal"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_deposit_negative_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,-15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            let result = engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Deposit amount must be greater than 0"),
                "Expected `Deposit amount must be greater than 0` error."
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should remain 10"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should remain 10"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_withdrawal_negative_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,-15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            let result = engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Withdrawal amount must be greater than 0"),
                "Expected `Withdrawal amount must be greater than 0` error."
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should remain 10"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should remain 10"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_withdrawal_from_zero_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,50.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap().to_owned();
    let engine = Arc::new(AsyncEngine::default());

    let handle = {
        let engine = Arc::clone(&engine);
        let input_path = input_path.clone();

        tokio::spawn(async move {
            let result = engine
                .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
                .await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Insufficient funds"),
                "Expected insufficient funds error"
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "Account should exist even if zero balance"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("10.0000").unwrap(),
                "Total should remain 10 after failed withdrawal attempt"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("10.0000").unwrap(),
                "Available should remain 10 after failed withdrawal attempt"
            );
            assert_eq!(
                account.held,
                Decimal::from_str("0.0000").unwrap(),
                "Held should be 0"
            );
        })
    };

    handle.await.unwrap();
}