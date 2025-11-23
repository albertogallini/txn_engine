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
                err.to_string()
                    .contains("Deposit amount must be greater than 0"),
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
                err.to_string()
                    .contains("Withdrawal amount must be greater than 0"),
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

#[tokio::test]
async fn unit_test_addition_overflow_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let large_amount = (Decimal::MAX / Decimal::from(2)).to_string();
    let csv_content = format!(
        r#"type,client,tx,amount,\n
                                deposit,1,1,{},\n
                                deposit,1,2,{},\n
                                withdrawal,1,3,{},\n"#,
        large_amount, large_amount, large_amount
    );

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

            assert!(
                result.is_err(),
                "Processing should fail due to addition overflow"
            );
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Addition overflow"),
                "Expected `Addition overflow` error"
            );

            assert_eq!(
                engine.accounts.len().await,
                1,
                "There should be one account"
            );
        })
    };

    handle.await.unwrap();
}

#[tokio::test]
async fn unit_test_decimal_precision_async() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,1.123456,\n
                                deposit,1,2,1.12345,\n
                                deposit,1,3,1.1234,\n
                                deposit,1,4,1.123,\n
                                deposit,1,5,1.12,\n
                                deposit,1,6,1.1,\n
                                deposit,1,7,1,\n
                                withdrawal,1,8,0.00045,\n
                                withdrawal,1,9,0.000045,\n
                                withdrawal,1,10,0.0000045,"#;

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

            // Note: This test originally expected failure on tiny withdrawals, but in correct engine logic,
            // 0.000045 and 0.0000045 should round to 0.0000 → withdrawal of 0 is rejected.
            // So we expect one "Withdrawal amount must be greater than 0" error.
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string()
                    .contains("Withdrawal amount must be greater than 0"),
                "Expected withdrawal of zero (after rounding) to be rejected"
            );

            let account_guard = engine.accounts.get(1).await.unwrap();
            let account = account_guard.get(&1).unwrap();

            assert_eq!(
                account.total,
                Decimal::from_str("7.7129").unwrap(),
                "Total should be 7.7129 after valid deposits and one valid tiny withdrawal"
            );
            assert_eq!(
                account.available,
                Decimal::from_str("7.7129").unwrap(),
                "Available should match total"
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
async fn unit_test_subtraction_overflow_async() {
    // Create temporary files for previous-session data
    let mut transactions_file = NamedTempFile::new().unwrap();
    let mut accounts_file = NamedTempFile::new().unwrap();

    // ---- Transactions from previous session (all valid) ----
    transactions_file
        .write_all(
            b"type,client,tx,amount\n\
              deposit,1,1,10.0000\n\
              deposit,2,2,5.0000\n\
              deposit,3,3,100.0000\n\
              withdrawal,1,4,5.0000\n",
        )
        .unwrap();

    // ---- Accounts dump with a malicious huge-negative amount for client 3 ----
    let huge_negative = rust_decimal::Decimal::MIN.to_string();
    // IMPORTANT: header must be exactly these 5 columns, NO trailing comma!
    let accounts_csv = format!(
        r#"client,available,held,total,locked
            1,5.0000,0.0000,5.0000,false
            2,5.0000,0.0000,5.0000,false
            3,{0},{0},{0},false"#,
        huge_negative
    );

    write!(accounts_file, "{}", accounts_csv).unwrap();

    let transactions_path = transactions_file.path().to_str().unwrap().to_owned();
    let accounts_path = accounts_file.path().to_str().unwrap().to_owned();

    let engine = Arc::new(AsyncEngine::new());

    let handle = {
        let engine = Arc::clone(&engine);
        let transactions_path = transactions_path.clone();
        let accounts_path = accounts_path.clone();

        tokio::spawn(async move {
            // 1. Load the "corrupted" state from previous session
            engine
                .load_from_previous_session_csvs(&transactions_path, &accounts_path)
                .await
                .expect("Failed to load from previous-session CSVs");

            // 2. Now send a dispute on the malicious deposit → subtraction overflow
            let mut dispute_file = NamedTempFile::new().unwrap();
            let dispute_csv = "type,client,tx,amount\n\
                               dispute,3,3,\n";
            write!(dispute_file, "{}", dispute_csv).unwrap();
            let dispute_path = dispute_file.path().to_str().unwrap().to_owned();

            let result = engine
                .read_and_process_transactions_from_csv(&dispute_path, BUFFER_SIZE)
                .await;

            // Expected: subtraction overflow when trying to hold the huge negative amount
            assert!(
                result.is_err(),
                "Dispute should fail with subtraction overflow"
            );
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("Subtraction overflow"),
                "Expected `Subtraction overflow` error, got: {}",
                err
            );
        })
    };

    handle.await.unwrap();
}
