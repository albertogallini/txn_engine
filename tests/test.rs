use rust_decimal::Decimal;
use std::str::FromStr;
use txn_engine::datastr::transaction::{TransactionProcessingError, TransactionType};
use txn_engine::engine::Engine;
use txn_engine::utility::read_and_process_csv_file; // Note the path adjustment if needed

use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn unit_test_deposit_and_withdrawal() {
    // Create a temporary CSV file
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1, 1, 10.0000,\n
                                withdrawal, 1, 2, 5.0000,\n"#;

    // Write content to the temporary file
    write!(temp_file, "{}", csv_content).unwrap();

    // Get the path of the temporary file
    let input_path = temp_file.path().to_str().unwrap();

    // Initialize the engine and process transactions
    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }
    // Assertions
    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account.total,
        Decimal::from_str("5.0000").unwrap(),
        "Total should be 5 after deposit and withdrawal"
    );
    assert_eq!(
        account.available,
        Decimal::from_str("5.0000").unwrap(),
        "Available should match total since no disputes"
    );
    assert_eq!(
        account.held,
        Decimal::from_str("0.0000").unwrap(),
        "Held should be 0"
    );

    // The temp file will be automatically deleted when `temp_file` goes out of scope
}

#[test]
fn unit_test_deposit_and_dispute() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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
}

#[test]
fn unit_test_dispute_deposit_after_withdrawal() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                withdrawal,1,3,20.0000,\n;
                                dispute,1,2,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(
        engine.accounts.len(),
        1,
        "Account should exist even if zero balance"
    );
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account.total,
        Decimal::from_str("10.0000").unwrap(),
        "Total should be 10 after failed withdrawal attempt"
    );
    assert_eq!(
        account.available,
        Decimal::from_str("-10.0000").unwrap(),
        "Available should be -10 after failed withdrawal attempt"
    );
    assert_eq!(
        account.held,
        Decimal::from_str("20.0000").unwrap(),
        "Held should be 20"
    );
}

#[test]
fn unit_test_deposit_and_dispute_resolve() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n
                                resolve,1,1,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account.total,
        Decimal::from_str("10.0000").unwrap(),
        "Total should remain 10 after dispute"
    );
    assert_eq!(
        account.available,
        Decimal::from_str("10.0000").unwrap(),
        "Available should be 0 after dispute"
    );
    assert_eq!(
        account.held,
        Decimal::from_str("0.0000").unwrap(),
        "Held should be 10 after dispute"
    );
}

#[test]
fn unit_test_deposit_and_dispute_chargeback() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n
                                chargeback,1,1,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account.total,
        Decimal::from_str("0.0000").unwrap(),
        "Total should remain 10 after dispute"
    );
    assert_eq!(
        account.available,
        Decimal::from_str("0.0000").unwrap(),
        "Available should be 0 after dispute"
    );
    assert_eq!(
        account.held,
        Decimal::from_str("0.0000").unwrap(),
        "Held should be 10 after dispute"
    );
    assert_eq!(
        account.locked, true,
        "Account must be locked after chargeback"
    );
}

#[test]
fn unit_test_deposit_withdrawal_dispute_withdrawal() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,5.0000,\n
                                dispute,1,2,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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

#[test]
fn unit_test_deposit_withdrawal_too_much() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();

    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file is expeceted to fail"),
        Err(e) => {
            assert!(
                e.to_string().contains("Insufficient funds"),
                "Expected `Insufficient funds` error"
            );
        }
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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
#[test]
fn unit_test_deposit_negative() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,-15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();

    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file is expeceted to fail"),
        Err(e) => {
            assert!(
                e.to_string()
                    .contains("Deposit amount must be greater than 0"),
                "Expected `Deposit amount must be greater than 0` error."
            );
        }
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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

#[test]
fn unit_test_withdrawal_negative() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,-15.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();

    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file is expeceted to fail"),
        Err(e) => {
            assert!(
                e.to_string()
                    .contains("Withdrawal amount must be greater than 0"),
                "Expected `Withdrawal amount must be greater than 0` error."
            );
        }
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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

#[test]
fn unit_test_withdrawal_from_zero() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                withdrawal,1,2,50.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file is expeceted to fail"),
        Err(e) => {
            println!("Error: {}", e);
            assert!(
                e.to_string().contains("Insufficient funds"),
                "Expected insufficient funds error"
            );
        }
    }

    assert_eq!(
        engine.accounts.len(),
        1,
        "Account should exist even if zero balance"
    );
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account.total,
        Decimal::from_str("10.0000").unwrap(),
        "Total should be 0 after failed withdrawal attempt"
    );
    assert_eq!(
        account.available,
        Decimal::from_str("10.0000").unwrap(),
        "Available should be 0 after failed withdrawal attempt"
    );
    assert_eq!(
        account.held,
        Decimal::from_str("0.0000").unwrap(),
        "Held should be 0"
    );
}

#[test]
fn unit_test_addition_overflow() {
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
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();

    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file should fail due to overflow"),
        Err(e) => {
            println!("{}", e.to_string());
            assert!(
                e.to_string().contains("Addition overflow"),
                "Expected `Addition overflow` error"
            );
        }
    }

    // Assertions about the state after the overflow attempt:
    assert_eq!(engine.accounts.len(), 1, "There should be one account");
}

#[test]
fn test_from_csv_file_basic() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_basic.csv";
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }

    println!("Accounts: {:#?}", engine.accounts);

    assert_eq!(engine.accounts.len(), 2, "Expected two accounts");

    let account1 = engine.accounts.get(&1).expect("Account 1 should exist");
    assert_eq!(
        account1.total,
        Decimal::from_str("10").unwrap(),
        "Account 1 total should be 10"
    );
    assert_eq!(
        account1.total, account1.available,
        "Account 1 total should equal available"
    );
    assert_eq!(
        account1.held,
        Decimal::from_str("0").unwrap(),
        "Account 1 held should be 0"
    );

    let account2 = engine.accounts.get(&2).expect("Account 2 should exist");
    assert_eq!(
        account2.total,
        Decimal::from_str("5").unwrap(),
        "Account 2 total should be 5"
    );
    assert_eq!(
        account2.total, account2.available,
        "Account 2 total should equal available"
    );
    assert_eq!(
        account2.held,
        Decimal::from_str("0").unwrap(),
        "Account 2 held should be 0"
    );
}

#[test]
fn test_from_csv_file_disputed() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_disputed.csv";
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }
    println!("Accounts: {:#?}", engine.accounts);

    assert_eq!(engine.accounts.len(), 6, "Expected six accounts");

    let account3 = engine.accounts.get(&3).expect("Account 3 should exist");
    assert_eq!(
        account3.total,
        Decimal::from_str("100").unwrap(),
        "Account 3 total should be 100"
    );
    assert!(!account3.locked, "Account 3 should not be locked");

    let account5 = engine.accounts.get(&5).expect("Account 5 should exist");
    assert_eq!(
        account5.total,
        Decimal::from_str("0").unwrap(),
        "Account 5 total should be 0"
    );
    assert!(account5.locked, "Account 5 should be locked");

    let account4 = engine.accounts.get(&4).expect("Account 4 should exist");
    assert_eq!(
        account4.total,
        Decimal::from_str("0").unwrap(),
        "Account 4 total should be 0"
    );
    assert!(account4.locked, "Account 4 should be locked");

    let account10 = engine.accounts.get(&10).expect("Account 10 should exist");
    assert_eq!(
        account10.total,
        Decimal::from_str("80").unwrap(),
        "Account 10 total should be 80"
    );
    assert_eq!(
        account10.held,
        Decimal::from_str("-20").unwrap(),
        "Account 10 held should be -20"
    );

    let account20 = engine.accounts.get(&20).expect("Account 20 should exist");
    assert_eq!(
        account20.total,
        Decimal::from_str("80").unwrap(),
        "Account 20 total should be 80"
    );
    assert_eq!(
        account20.held,
        Decimal::from_str("0").unwrap(),
        "Account 20 held should be 0"
    );

    let account30 = engine.accounts.get(&30).expect("Account 30 should exist");
    assert_eq!(
        account30.total,
        Decimal::from_str("120").unwrap(),
        "Account 30 total should be 120"
    );
    assert_eq!(
        account30.held,
        Decimal::from_str("20").unwrap(),
        "Account 30 held should be 20"
    );
}

#[test]
///Tests the handling of erroneous transactions from a CSV file.

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
/// withdrawal ,9     ,21  ,200            # Insufficient funds : the transaction does NOT gets into the transaaction log.
/// dispute    ,9     ,21  ,               # Dispute on non-existent or invalid tx
/// The test checks that the correct errors are reported.
///
fn test_from_csv_file_error_conditions() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_errors.csv";
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("Expected an error, but got success"),
        Err(TransactionProcessingError::MultipleErrors(errors)) => {
            let expected_errors = vec![
                "Error reading transaction record: Unknown transaction type: DEPOSIT",
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
    assert_eq!(engine.accounts.len(), 4);
}

#[test]
fn test_from_csv_file_decimal_precision() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_digits.csv";
    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!("Some error occurred while processing transactions: {}", e),
    }
    println!("Accounts: {:#?}", engine.accounts);

    // Check if we have processed transactions for exactly one client
    assert_eq!(
        engine.accounts.len(),
        1,
        "Should have processed transactions for one client"
    );

    // Check specific account details
    let account = engine.accounts.get(&1).expect("Account 1 should exist");

    // Here are assertions for each transaction based on the expected rounding:
    assert_eq!(account.total, Decimal::from_str("7.7129").unwrap());
    assert_eq!(account.available, Decimal::from_str("7.7129").unwrap());
    assert_eq!(account.held, Decimal::from_str("0.0000").unwrap());
}

#[test]
fn test_load_from_previous_session_csv() {
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
    let mut engine = Engine::new();

    // Load data from CSV files
    engine
        .load_from_previous_session_csvs(
            transactions_file.path().to_str().unwrap(),
            accounts_file.path().to_str().unwrap(),
        )
        .expect("Failed to load from CSV");

    // Check if transactions were loaded correctly
    assert_eq!(engine.transaction_log.len(), 3);
    let tx1 = engine.transaction_log.get(&1).unwrap();
    assert_eq!(tx1.ty, TransactionType::Deposit);
    assert_eq!(tx1.client, 1);
    assert_eq!(tx1.tx, 1);
    assert_eq!(tx1.amount, Some(Decimal::new(10_0000, 4))); // 10.0000

    let tx2 = engine.transaction_log.get(&3).unwrap();
    assert_eq!(tx2.ty, TransactionType::Withdrawal);
    assert_eq!(tx2.client, 1);
    assert_eq!(tx2.tx, 3);
    assert_eq!(tx2.amount, Some(Decimal::new(5_0000, 4))); // 5.0000

    // Check if accounts were loaded correctly
    assert_eq!(engine.accounts.len(), 2);
    let account = engine.accounts.get(&1).unwrap();
    assert_eq!(account.available, Decimal::new(5_0000, 4)); // 5.0000
    assert_eq!(account.held, Decimal::new(0, 4)); // 0.0000
    assert_eq!(account.total, Decimal::new(5_0000, 4)); // 5.0000
    assert!(!account.locked);
}

/// Tests the handling of subtraction overflow during transaction processing.
///
/// This test simulates a scenario where a dispute transaction causes a subtraction
/// overflow. It creates temporary CSV files for transactions and accounts, loads them
/// into an `Engine` instance, and then processes a transaction that should trigger
/// a subtraction overflow error.
/// This is necessary as the engine cannot generate a status on the Engine such that a
/// transaction can generate a subtraction overflow. So we need to populate the Engine state
/// from a "corrupted" input file.
///
/// The test expects the `read_and_process_csv_file` function to return an error
/// indicating a `Subtraction overflow`. If no error occurs or a different error
/// is returned, the test will fail.
#[test]
fn test_subrtaction_overflow() {
    // Create temporary files for transactions and accounts
    let mut transactions_file = NamedTempFile::new().expect("Failed to create temporary file");
    let mut accounts_file = NamedTempFile::new().expect("Failed to create temporary file");

    // Write transaction data
    transactions_file
        .write_all(
            b"type,client,tx,amount\n
                                        deposit,1,1,10.0000\n
                                        deposit,2,2,5.0000\n
                                        deposit,3,3,100.0000\n
                                        withdrawal,1,4,5.0000",
        )
        .unwrap();

    let large_neg_amount = (Decimal::MIN + Decimal::from(1)).to_string();
    let csv_content = format!(
        r#"client,available,held,total,locked,\n
        1,5.0000,0.0000,5.0000,false,\n
        2,5.0000,0.0000,5.0000,false,\n
        3,{},{},{},false,\n"#,
        large_neg_amount, large_neg_amount, large_neg_amount
    );

    // Write account data
    write!(accounts_file, "{}", csv_content).unwrap();

    // Create an instance of Engine
    let mut engine = Engine::new();

    // Load data from CSV files
    engine
        .load_from_previous_session_csvs(
            transactions_file.path().to_str().unwrap(),
            accounts_file.path().to_str().unwrap(),
        )
        .expect("Failed to load from CSV");

    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = format!(
        r#"type,client,tx,amount,\n
           dispute,3,3,,\n"#
    );
    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    match read_and_process_csv_file(&mut engine, input_path) {
        Ok(()) => panic!("read_and_process_csv_file should fail due to overflow"),
        Err(e) => {
            println!("{}", e.to_string());
            assert!(
                e.to_string().contains("Subtraction overflow"),
                "Expected `Subtraction overflow` error"
            );
        }
    }
}
