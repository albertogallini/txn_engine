use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;
use txn_engine::{
    asyncengine::{AsyncEngine, AsyncEngineFunctions},
    datastr::transaction::TransactionProcessingError,
    utility::generate_random_transaction_concurrent_stream,
};

use std::io::Write;
use txn_engine::datastr::transaction::TransactionType;

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

    let result = engine
        .read_and_process_transactions_from_csv(&input_path, BUFFER_SIZE)
        .await;

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
}

#[tokio::test]
async fn unit_test_subtraction_overflow_async() {
    let mut transactions_file = NamedTempFile::new().unwrap();
    let mut accounts_file = NamedTempFile::new().unwrap();

    transactions_file
        .write_all(
            b"type,client,tx,amount\n\
              deposit,1,1,10.0000\n\
              deposit,2,2,5.0000\n\
              deposit,3,3,100.0000\n\
              withdrawal,1,4,5.0000\n",
        )
        .unwrap();

    let huge_negative = rust_decimal::Decimal::MIN.to_string();
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

    engine
        .load_from_previous_session_csvs(&transactions_path, &accounts_path)
        .await
        .expect("Failed to load from previous-session CSVs");

    let mut dispute_file = NamedTempFile::new().unwrap();
    let dispute_csv = "type,client,tx,amount\n\
                       dispute,3,3,\n";
    write!(dispute_file, "{}", dispute_csv).unwrap();
    let dispute_path = dispute_file.path().to_str().unwrap().to_owned();

    let result = engine
        .read_and_process_transactions_from_csv(&dispute_path, BUFFER_SIZE)
        .await;

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
}

#[tokio::test]
async fn reg_test_from_csv_file_basic_async() {
    let input_path = "tests/transactions_basic.csv";
    let engine = Arc::new(AsyncEngine::new());

    match engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await
    {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len().await, 2, "There should be 2 accounts");

    let account_guard = engine.accounts.get(1).await.unwrap();
    let account1 = account_guard.get(&1).unwrap();

    assert_eq!(
        account1.total,
        Decimal::from_str("10.0001").unwrap(),
        "Account 1 total should be 10.0001"
    );
    assert_eq!(
        account1.total, account1.available,
        "Account 1 total should equal available"
    );
    assert_eq!(account1.held, Decimal::ZERO, "Account 1 held should be 0");

    let account_guard = engine.accounts.get(2).await.unwrap();
    let account2 = account_guard.get(&2).unwrap();
    assert_eq!(
        account2.total,
        Decimal::from_str("5").unwrap(),
        "Account 2 total should be 5"
    );
    assert_eq!(
        account2.total, account2.available,
        "Account 2 total should equal available"
    );
    assert_eq!(account2.held, Decimal::ZERO, "Account 2 held should be 0");
}

#[tokio::test]
async fn reg_test_from_csv_file_disputed_async() {
    let engine = AsyncEngine::default();
    let input_path = "tests/transactions_disputed.csv";
    match engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await
    {
        Ok(()) => {}
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len().await, 6, "Expected six accounts");

    let account3 = engine
        .accounts
        .get(3)
        .await
        .expect("Account 3 should exist");
    assert_eq!(
        account3.get(&3).unwrap().total,
        Decimal::from_str("100").unwrap(),
        "Account 3 total should be 100"
    );
    assert!(
        !account3.get(&3).unwrap().locked,
        "Account 3 should not be locked"
    );

    let account5 = engine
        .accounts
        .get(5)
        .await
        .expect("Account 5 should exist");
    assert_eq!(
        account5.get(&5).unwrap().total,
        Decimal::from_str("0").unwrap(),
        "Account 5 total should be 0"
    );
    assert!(
        account5.get(&5).unwrap().locked,
        "Account 5 should be locked"
    );

    let account4 = engine
        .accounts
        .get(4)
        .await
        .expect("Account 4 should exist");
    assert_eq!(
        account4.get(&4).unwrap().total,
        Decimal::from_str("0").unwrap(),
        "Account 4 total should be 0"
    );
    assert!(
        account4.get(&4).unwrap().locked,
        "Account 4 should be locked"
    );

    let account10 = engine
        .accounts
        .get(10)
        .await
        .expect("Account 10 should exist");
    assert_eq!(
        account10.get(&10).unwrap().total,
        Decimal::from_str("80").unwrap(),
        "Account 10 total should be 80"
    );
    assert_eq!(
        account10.get(&10).unwrap().held,
        Decimal::from_str("-20").unwrap(),
        "Account 10 held should be -20"
    );

    let account20 = engine
        .accounts
        .get(20)
        .await
        .expect("Account 20 should exist");
    assert_eq!(
        account20.get(&20).unwrap().total,
        Decimal::from_str("80").unwrap(),
        "Account 20 total should be 80"
    );
    assert_eq!(
        account20.get(&20).unwrap().held,
        Decimal::from_str("0").unwrap(),
        "Account 20 held should be 0"
    );

    let account30 = engine
        .accounts
        .get(30)
        .await
        .expect("Account 30 should exist");
    assert_eq!(
        account30.get(&30).unwrap().total,
        Decimal::from_str("120").unwrap(),
        "Account 30 total should be 120"
    );
    assert_eq!(
        account30.get(&30).unwrap().held,
        Decimal::from_str("20").unwrap(),
        "Account 30 held should be 20"
    );
}

///Tests the handling of several erroneous transactions from a CSV file.

/// type       ,client,tx   ,amount

/// deposit    ,6     ,9    ,0.0000
/// withdrawal ,6     ,10   ,-5.0000       # Negative amount, should fail
/// deposit    ,6     ,11   ,79228162514264337593543950330  # Large amount
/// deposit    ,6     ,12   ,5000.0000     # Addition overflowed
/// withdrawal ,6     ,13   ,              # Empty amount --> fail

/// deposit    ,7     ,14   ,              # Empty amount --> fail
/// deposit    ,7     ,15   ,10.0
/// deposit    ,7     ,15   ,10.0          # Duplicate tx
/// dispute    ,7     ,16   ,              # Dispute on non-existent or invalid tx

/// resolve    ,6     ,9999 ,              # Resolve on non-existent tx

/// chargeback ,7     ,16   ,              # Chargeback on non-existent tx

/// dispute    ,7     ,15   ,
/// dispute    ,7     ,15   ,              # Transaction already disputed
/// chargeback ,7     ,15   ,
/// deposit    ,7     ,17   ,10            # Account is locked
/// deposit    ,8     ,18   ,10
/// resolve    ,8     ,18   ,              # Transaction not disputed
///
/// deposit    ,9     ,20  ,100
/// withdrawal ,9     ,21  ,200            # Insufficient funds : the transaction does NOT gets into the transaction log.
/// dispute    ,9     ,21  ,               # Dispute on non-existent or invalid tx
/// The test checks that the correct errors are reported.
///
#[tokio::test]
async fn reg_test_from_csv_file_error_conditions_async() {
    let engine = Arc::new(AsyncEngine::new());
    let input_path = "tests/transactions_errors.csv";

    // Call the method directly and await it — no spawn needed!
    let result = engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await;

    match result {
        Ok(()) => panic!("Expected an error, but got success"),
        Err(TransactionProcessingError::MultipleErrors(errors)) => {
            let expected_errors = vec![
                "Error reading transaction record: CSV deserialize error: record 18 (line: 19, byte: 335): Unknown transaction type: DEPOSIT",
                "Error processing Transaction { ty: Deposit, client: 6, tx: 9, amount: Some(0.0000), disputed: false }: Deposit amount must be greater than 0",
                "Error processing Transaction { ty: Withdrawal, client: 6, tx: 10, amount: Some(-5.0000), disputed: false }: Withdrawal amount must be greater than 0",
                "Error processing Transaction { ty: Deposit, client: 6, tx: 12, amount: Some(5000.0000), disputed: false }: Addition overflow",
                "Error processing Transaction { ty: Withdrawal, client: 6, tx: 13, amount: None, disputed: false }: Transaction must have an amount",
                "Error processing Transaction { ty: Deposit, client: 7, tx: 14, amount: None, disputed: false }: Transaction must have an amount",
                "Error processing Transaction { ty: Deposit, client: 7, tx: 15, amount: Some(10), disputed: false }: Transaction id already processed in this session - cannot be repeated.",
                "Error processing Transaction { ty: Dispute, client: 7, tx: 16, amount: None, disputed: false }: Transaction not found",
                "Error processing Transaction { ty: Resolve, client: 6, tx: 9999, amount: None, disputed: false }: Transaction not found",
                "Error processing Transaction { ty: Chargeback, client: 7, tx: 16, amount: None, disputed: false }: Transaction not found",
                "Error processing Transaction { ty: Dispute, client: 7, tx: 15, amount: None, disputed: false }: Transaction already disputed",
                "Error processing Transaction { ty: Deposit, client: 7, tx: 17, amount: Some(10), disputed: false }: Account is locked",
                "Error processing Transaction { ty: Resolve, client: 8, tx: 18, amount: None, disputed: false }: Transaction not disputed",
                "Error processing Transaction { ty: Withdrawal, client: 9, tx: 21, amount: Some(200), disputed: false }: Insufficient funds",
                "Error processing Transaction { ty: Dispute, client: 9, tx: 21, amount: None, disputed: false }: Transaction not found",
            ];

            let mut actual_errors = errors.iter().map(|e| e.to_string()).collect::<Vec<_>>();
            actual_errors.sort();
            let mut expected_errors_sorted = expected_errors
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            expected_errors_sorted.sort();

            assert_eq!(
                actual_errors, expected_errors_sorted,
                "Errors do not match expected errors"
            );

            assert_eq!(engine.accounts.len().await, 4);
        }
    }
}

/// Tests that processing a CSV file with malformed records results in the expected errors.
///
/// Verifies that:
/// 1. The `TransactionProcessingError::MultipleErrors` variant is returned.
/// 2. The errors are correctly sorted alphabetically.
/// 3. The accounts are correctly updated after the transactions are processed.
#[tokio::test]
async fn reg_test_from_csv_file_malformed_async() {
    let engine = Arc::new(AsyncEngine::new());
    let input_path = "tests/transactions_malformed.csv";

    match engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await
    {
        Ok(()) => panic!("Expected an error, but got success"),
        Err(TransactionProcessingError::MultipleErrors(errors)) => {
            let expected_errors = vec![
                "Error processing Transaction { ty: Withdrawal, client: 1, tx: 3, amount: Some(5.0000), disputed: false }: Account not found",
                "Error reading transaction record: CSV deserialize error: record 1 (line: 2, byte: 22): invalid digit found in string", 
                "Error reading transaction record: CSV deserialize error: record 2 (line: 3, byte: 46): Unknown transaction type: deposi",
                "Error reading transaction record: CSV deserialize error: record 4 (line: 5, byte: 94): Unknown transaction type: witawal"
            ];

            // Compare the sorted errors to ensure the order doesn't matter
            let mut actual_errors = errors.clone();
            actual_errors.sort();
            let mut expected_errors_sorted = expected_errors;
            expected_errors_sorted.sort();

            assert_eq!(
                actual_errors, expected_errors_sorted,
                "Errors do not match expected errors"
            );
        }
    }
    assert_eq!(engine.accounts.len().await, 1);
}

/// Tests loading transactions and accounts from CSV files into the `Engine`.
///
/// This test creates temporary CSV files for transactions and accounts,
/// writes predefined data into them, and then loads this data into an
/// `Engine` instance using the `load_from_previous_session_csvs` method.
///
/// It verifies that the transactions and accounts are correctly loaded by
/// asserting the number of entries and checking specific transaction and
/// account details.
///
/// The test expects that:
/// - The transaction log contains three transactions with correct details.
/// - The accounts map contains two accounts with expected balances and states.

#[tokio::test]
async fn reg_test_load_from_previous_session_csv_async() {
    // Create temporary files for transactions and accounts
    let mut transactions_file = NamedTempFile::new().expect("Failed to create temporary file");
    let mut accounts_file = NamedTempFile::new().expect("Failed to create temporary file");

    // Write transaction data
    transactions_file
        .write_all(
            b"type,client,tx,amount\n
                                        deposit,1,1,10.0000\n
                                        deposit,2,2,5.0000\n
                                        withdrawal,1,3,5.0000",
        )
        .unwrap();

    // Write account data
    accounts_file
        .write_all(
            b"client,available,held,total,locked\n
                                   1,5.0000,0.0000,5.0000,false\n
                                   2,5.0000,0.0000,5.0000,false",
        )
        .unwrap();

    // Create an instance of Engine
    let engine = Arc::new(AsyncEngine::new());

    let handle = {
        tokio::spawn(async move {
            // Load data from CSV files
            engine
                .load_from_previous_session_csvs(
                    transactions_file.path().to_str().unwrap(),
                    accounts_file.path().to_str().unwrap(),
                )
                .await
                .expect("Failed to load from CSV");

            let account_guard = engine.accounts.get(2).await.unwrap();
            let account2 = account_guard.get(&2).unwrap();
            assert_eq!(
                account2.total,
                Decimal::from_str("5").unwrap(),
                "Account 2 total should be 5"
            );

            // Check if transactions were loaded correctly
            assert_eq!(engine.transaction_log.len().await, 3);

            let tx1_guard = engine.transaction_log.get(1).await.unwrap();
            let tx1 = tx1_guard.get(&1).unwrap();
            assert_eq!(tx1.ty, TransactionType::Deposit);
            assert_eq!(tx1.client, 1);
            assert_eq!(tx1.tx, 1);
            assert_eq!(tx1.amount, Some(Decimal::new(10_0000, 4))); // 10.0000

            let tx2_guard = engine.transaction_log.get(3).await.unwrap();
            let tx2 = tx2_guard.get(&3).unwrap();
            assert_eq!(tx2.ty, TransactionType::Withdrawal);
            assert_eq!(tx2.client, 1);
            assert_eq!(tx2.tx, 3);
            assert_eq!(tx2.amount, Some(Decimal::new(5_0000, 4))); // 5.0000

            // Check if accounts were loaded correctly
            assert_eq!(engine.accounts.len().await, 2);
            let account_guard = engine.accounts.get(2).await.unwrap();
            let account = account_guard.get(&2).unwrap();
            assert_eq!(account.available, Decimal::new(5_0000, 4)); // 5.0000
            assert_eq!(account.held, Decimal::new(0, 4)); // 0.0000
            assert_eq!(account.total, Decimal::new(5_0000, 4)); // 5.0000
            assert!(!account.locked);
        })
    };

    //wait for completion
    handle.await.unwrap();
}

/// Tests serialization and deserialization of the `Engine` to and from CSV files.
///
/// This test creates a temporary file for transactions and accounts,
/// writes predefined data into them, and then loads this data into an
/// `Engine` instance using the `load_from_previous_session_csvs` method.
///
/// It then dumps the `Engine` state to a temporary file using the
/// `dump_transaction_log_to_csvs` method and loads the data from the
/// temporary file into another `Engine` instance.
///
/// It verifies that the transactions and accounts are correctly loaded by
/// asserting the number of entries and checking specific transaction and
/// account details.
#[tokio::test]
async fn reg_test_serdesr_engine() -> Result<(), Box<dyn std::error::Error>> {
    use tempfile::NamedTempFile;
    use tokio::fs;

    let engine = Arc::new(AsyncEngine::new());
    let input_path = "tests/transactions_mixed.csv";

    match engine
        .read_and_process_transactions_from_csv(input_path, BUFFER_SIZE)
        .await
    {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!("Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len().await, 4, "Expected four accounts");

    // === Dump transaction log ===
    let transactions_file = NamedTempFile::new()?;
    engine
        .dump_transaction_log_to_csv(transactions_file.path().to_str().unwrap(), BUFFER_SIZE)
        .await?;

    // === Dump accounts ===
    let accounts_file = NamedTempFile::new()?;
    let tokio_file = fs::File::create(accounts_file.path()).await?; // now ? works!
    engine.dump_account_to_csv(tokio_file, BUFFER_SIZE).await?;

    let engine2 = Arc::new(AsyncEngine::new());

    // Deserialize transactions from temp file into engine2
    match engine2
        .load_from_previous_session_csvs(
            transactions_file.path().to_str().unwrap(),
            accounts_file.path().to_str().unwrap(),
        )
        .await
    {
        Ok(()) => {}
        Err(e) => println!(
            "Some error occurred loading the engine from previous dump: {}",
            e
        ),
    }

    let mut original_stream = engine.accounts.iter().await;
    let mut loaded_stream = engine2.accounts.iter().await;

    let mut original_vec = vec![];
    let mut loaded_vec = vec![];

    while let Some((id, guard)) = original_stream.next().await {
        if let Some(acc) = guard.get(&id) {
            original_vec.push((id, acc.clone()));
        }
    }
    while let Some((id, guard)) = loaded_stream.next().await {
        if let Some(acc) = guard.get(&id) {
            loaded_vec.push((id, acc.clone()));
        }
    }

    original_vec.sort_by_key(|(id, _)| *id);
    loaded_vec.sort_by_key(|(id, _)| *id);

    assert_eq!(
        original_vec, loaded_vec,
        "Accounts do not match after round-trip"
    );

    let mut original_txs: Vec<_> = vec![];
    let mut original_tx_iter = engine.transaction_log.iter().await;
    while let Some((tx_id, guard)) = original_tx_iter.next().await {
        if let Some(tx) = guard.get(&tx_id) {
            original_txs.push((tx_id, tx.clone()));
        }
    }

    let mut loaded_txs: Vec<_> = vec![];
    let mut loaded_tx_iter = engine2.transaction_log.iter().await;
    while let Some((tx_id, guard)) = loaded_tx_iter.next().await {
        if let Some(tx) = guard.get(&tx_id) {
            loaded_txs.push((tx_id, tx.clone()));
        }
    }

    original_txs.sort_by_key(|(id, _)| *id);
    loaded_txs.sort_by_key(|(id, _)| *id);

    assert_eq!(
        original_txs, loaded_txs,
        "Transaction logs do not match after round-trip"
    );

    Ok(()) // ← important!
}

/// Test that the engine produces consistent results even when processing transactions concurrently.
/// This test is not exhaustive in terms of transaction type coverage, but provides a reasonable
/// level of confidence that the engine is thread-safe and can handle concurrent transaction processing.
///
/// The test creates two engines, `engine1` and `engine2`. It processes three files sequentially
/// with `engine1` and concurrently with `engine2`. It then compares the account snapshots of
/// `engine1` and `engine2` to ensure they are equal.
///
/// Transactions are generated in a way that every file contains a disjoint set of client IDs and tx IDs,
/// so concurrent executions (i.e. with different transaction execution orders) won't cause different
/// final amounts in the client accounts (if the engine manage the concurrency correctly).
#[tokio::test]
async fn reg_test_engine_consistency_with_concurrent_processing_async(
) -> Result<(), Box<dyn std::error::Error>> {
    const BUF_SIZE: usize = 1024;
    // Generate 3 temp files with consistent random transactions
    let temp_file1 = generate_random_transaction_concurrent_stream(1_000_000, 0, 1, 10).unwrap();
    let temp_file2 =
        generate_random_transaction_concurrent_stream(1_000_000, 1_000_001, 200, 300).unwrap();
    let temp_file3 =
        generate_random_transaction_concurrent_stream(1_000_000, 2_000_001, 400, 500).unwrap();

    /* debug
    let mut writer = Writer::from_writer(std::io::stdout());
    let mut source = File::open(temp_file1.path()).unwrap();
    let mut destination = File::create("tempfile1.csv").unwrap();
    std::io::copy(&mut source, &mut destination).unwrap();
    let mut source = File::open(temp_file2.path()).unwrap();
    let mut destination = File::create("tempfile2.csv").unwrap();
    std::io::copy(&mut source, &mut destination).unwrap();
    let mut source = File::open(temp_file3.path()).unwrap();
    let mut destination = File::create("tempfile3.csv").unwrap();
    std::io::copy(&mut source, &mut destination).unwrap(); */

    // Create two engines
    let engine_seq = Arc::new(AsyncEngine::new());
    let engine_concurrent = Arc::new(AsyncEngine::new());

    // Process files sequentially with engine1
    match engine_seq
        .read_and_process_transactions(File::open(temp_file1.path()).await.unwrap(), BUF_SIZE)
        .await
    {
        Ok(()) => {}
        Err(..) => {}
    }
    match engine_seq
        .read_and_process_transactions(File::open(temp_file2.path()).await.unwrap(), BUF_SIZE)
        .await
    {
        Ok(()) => {}
        Err(..) => {}
    }
    match engine_seq
        .read_and_process_transactions(File::open(temp_file3.path()).await.unwrap(), BUF_SIZE)
        .await
    {
        Ok(()) => {}
        Err(..) => {}
    }

    // Process files concurrently with engine2
    let handles = vec![
        {
            let engine_concurrent = Arc::clone(&engine_concurrent);
            let file = File::open(temp_file1.path()).await.unwrap();
            tokio::spawn(async move {
                match engine_concurrent
                    .read_and_process_transactions(file, BUF_SIZE)
                    .await
                {
                    Ok(()) => {}
                    Err(..) => {}
                };
            })
        },
        {
            let engine_concurrent = Arc::clone(&engine_concurrent);
            let file = File::open(temp_file2.path()).await.unwrap();
            tokio::spawn(async move {
                match engine_concurrent
                    .read_and_process_transactions(file, BUF_SIZE)
                    .await
                {
                    Ok(()) => {}
                    Err(..) => {}
                };
            })
        },
        {
            let engine_concurrent = Arc::clone(&engine_concurrent);
            let file = File::open(temp_file3.path()).await.unwrap();
            tokio::spawn(async move {
                match engine_concurrent
                    .read_and_process_transactions(file, BUF_SIZE)
                    .await
                {
                    Ok(()) => {}
                    Err(..) => {}
                };
            })
        },
    ];

    // Wait for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

    /*  debug
    writer.write_record(["client", "available", "held", "total", "locked"]).unwrap();
    writer.flush().unwrap();
    let _ = serialize_account_balances_csv(&engine1.accounts, std::io::stdout());
    writer.write_record(["client", "available", "held", "total", "locked"]).unwrap();
    writer.flush().unwrap();
    let _ = serialize_account_balances_csv(&engine2.accounts, std::io::stdout()); */

    let mut accounts_seq = Vec::new();
    let mut iter = engine_seq.accounts.iter().await;
    while let Some((id, guard)) = iter.next().await {
        if let Some(acc) = guard.get(&id) {
            accounts_seq.push((id, acc.clone()));
        }
    }

    let mut accounts_concurrent = Vec::new();
    let mut iter = engine_concurrent.accounts.iter().await;
    while let Some((id, guard)) = iter.next().await {
        if let Some(acc) = guard.get(&id) {
            accounts_concurrent.push((id, acc.clone()));
        }
    }

    accounts_seq.sort_by_key(|(id, _)| *id);
    accounts_seq.sort_by_key(|(id, _)| *id);
    assert_eq!(accounts_seq, accounts_seq, "Account states differ");

    Ok(())
}
