use csv::Writer;
use rust_decimal::Decimal;
use std::fs::File;
use std::str::FromStr;
use txn_engine::datastr::account::serialize_account_balances_csv;
use txn_engine::datastr::transaction::{TransactionProcessingError, TransactionType};
use txn_engine::engine::{Engine, EngineFunctions};
use txn_engine::utility::generate_random_transaction_concurrent_stream;

use std::io::Write;
use tempfile::NamedTempFile;

const BUFFER_SIZE: usize = 16_384;

#[test]
fn unit_test_deposit_and_withdrawal() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1, 1, 10.0000,\n
                                withdrawal, 1, 2, 5.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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
}

#[test]
fn unit_test_double_dispute() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                withdrawal,1,3,5.0000,\n;
                                dispute,1,3,,\n
                                dispute,1,2,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {}
        Err(e) => eprintln!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 1, "There should be one account");
    let account = engine.accounts.get(&1).expect("Account 1 should exist");
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

#[test]
fn unit_test_txid_reused_after_dispute_and_resolve() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                deposit,1,2,20.0000,\n
                                dispute,1,2,,\n#
                                resolve,1,2,,\n
                                deposit,1,2,20.0000,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {
            panic!("Engine::unit_test_txid_reused_after_dispute_and_resolve is expeceted to fail");
        }
        Err(e) => {
            assert!(
            e.to_string().contains("{ ty: Deposit, client: 1, tx: 2, amount: Some(20.0000), disputed: false }: Transaction id already processed in this session - cannot be repeated"),
            "Expected `ty: Deposit, client: 1, tx: 2, amount: Some(20.0000), disputed: false : Transaction id already processed in this session - cannot be repeated` error"
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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

    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Engine::read_and_process_transactions_from_csv is expeceted to fail"),
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

    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Engine::read_and_process_transactions_from_csv is expeceted to fail"),
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

    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Engine::read_and_process_transactions_from_csv is expeceted to fail"),
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
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Engine::read_and_process_transactions_from_csv is expeceted to fail"),
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

    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {
            panic!("Engine::read_and_process_transactions_from_csv should fail due to overflow")
        }
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

/// Test that transactions with decimal amounts are processed correctly, including
/// rounding at the right precision.
///
/// The test checks that the total and available amounts are correct, and that
/// there are no held funds.
/// Deposits:
/// 1.123456 => 1.1235 (rounded up)
/// 1.12345 => 1.1235 (rounded up)
/// 1.1234 => 1.1234 (no change)
/// 1.123 => 1.1230 (adding trailing zeros for consistency)
/// 1.12 => 1.1200
/// 1.1 => 1.1000
/// 1 => 1.0000

/// Sum of deposits = 1.1235 + 1.1235 + 1.1234 + 1.1230 + 1.1200 + 1.1000 + 1.0000 = 7.7134
/// Withdrawals:
/// 0.00045 => 0.0005 (rounded up)
/// 0.000045 => 0.0000 (rounded down to zero due to 4-digit precision)
/// 0.0000045 => 0.0000 (rounded down to zero due to 4-digit precision)

/// Sum of withdrawals = 0.0005 + 0.0000 + 0.0000 = 0.0005
/// Net Balance Calculation:
/// Net Balance = Sum of Deposits - Sum of Withdrawals
/// = 7.7134 - 0.0005
/// = 7.7129
///
#[test]
fn unit_test_decimal_precision() {
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
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Engine::read_and_process_transactions_from_csv is expeceted to fail"),
        Err(e) => {
            println!("Error: {}", e);
            assert!(
                e.to_string()
                    .contains("Withdrawal amount must be greater than 0"),
                "Expected `Withdrawal amount must be greater than 0`error"
            );
        }
    }

    // Check specific account details
    let account = engine.accounts.get(&1).expect("Account 1 should exist");

    // Here are assertions for each transaction based on the expected rounding:
    assert_eq!(account.total, Decimal::from_str("7.7129").unwrap());
    assert_eq!(account.available, Decimal::from_str("7.7129").unwrap());
    assert_eq!(account.held, Decimal::from_str("0.0000").unwrap());
}

/// Tests the handling of subtraction overflow during transaction processing.
///
/// This test simulates a scenario where a dispute transaction causes a subtraction
/// overflow. It creates temporary CSV files for transactions and accounts dumps, loads them
/// into an `Engine` instance, and then processes a transaction that should trigger
/// a subtraction overflow error.
/// This is necessary as the engine cannot generate a status on the Engine such that a
/// transaction can generate a subtraction overflow just processing transactions. So we need to populate the Engine state
/// from an ad-hoc "corrupted" input file.
///
/// The test expects the `Engine::read_and_process_transactions_from_csv` function to return an error
/// indicating a `Subtraction overflow`. If no error occurs or a different error
/// is returned, the test will fail.
#[test]
fn unit_test_subrtaction_overflow() {
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

    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {
            panic!("Engine::read_and_process_transactions_from_csv should fail due to overflow")
        }
        Err(e) => {
            println!("{}", e.to_string());
            assert!(
                e.to_string().contains("Subtraction overflow"),
                "Expected `Subtraction overflow` error"
            );
        }
    }
}

/// Test that transactions are processed correctly from a CSV file.
///
/// The CSV file `tests/transactions_basic.csv` contains three deposits and two withdrawal.
/// After processing, the Engine should have two accounts with the correct total and available funds.
///
#[test]
fn reg_test_from_csv_file_basic() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_basic.csv";
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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

/// Tests the processing of transactions from a CSV file, specifically focusing on the dispute
/// and chargeback transactions. This test checks that the accounts are correctly updated
/// after the dispute and chargeback transactions are processed.
#[test]
fn reg_test_from_csv_file_disputed() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_disputed.csv";
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => {}
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }

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
fn reg_test_from_csv_file_error_conditions() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_errors.csv";
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
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

/// Tests that processing a CSV file with malformed records results in the expected errors.
///
/// Verifies that:
/// 1. The `TransactionProcessingError::MultipleErrors` variant is returned.
/// 2. The errors are correctly sorted alphabetically.
/// 3. The accounts are correctly updated after the transactions are processed.
#[test]
fn reg_test_from_csv_file_malformed() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_malformed.csv";
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => panic!("Expected an error, but got success"),
        Err(TransactionProcessingError::MultipleErrors(errors)) => {
            let expected_errors = vec![
                "Error reading transaction record: CSV deserialize error: record 1 (line: 2, byte: 22): invalid digit found in string",
                "Error reading transaction record: Unknown transaction type: deposi",
                "Error reading transaction record: Unknown transaction type: witawal",
                "Error processing Transaction { ty: Withdrawal, client: 1, tx: 3, amount: Some(5.0000), disputed: false }: Account not found",
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
    assert_eq!(engine.accounts.len(), 1);
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

#[test]
fn reg_test_load_from_previous_session_csv() {
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
#[test]
fn reg_test_serdesr_engine() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_mixed.csv";
    match engine.read_and_process_transactions_from_csv(input_path, BUFFER_SIZE) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }

    assert_eq!(engine.accounts.len(), 4, "Expected four accounts");

    {
        // Create temporary files for transactions
        let transactions_file = NamedTempFile::new().expect("Failed to create temporary file");
        // Use the temporary files for dumping session data
        match engine
            .dump_transaction_log_to_csv(transactions_file.path().to_str().unwrap(), BUFFER_SIZE)
        {
            Ok(()) => {}
            Err(e) => println!("Some error occurred dumping the engine: {}", e),
        }

        // Create temporary files for  accounts
        let accounts_file = NamedTempFile::new().expect("Failed to create temporary file");
        let mut writer = Writer::from_writer(&accounts_file);
        // Use the temporary files for dumping session data
        writer
            .write_record(["client", "available", "held", "total", "locked"])
            .unwrap();
        writer.flush().unwrap();
        let _ = serialize_account_balances_csv(&engine.accounts, &accounts_file);

        let mut engine2 = Engine::default();
        // Deserialize transactions from temp file into engine2
        match engine2.load_from_previous_session_csvs(
            transactions_file.path().to_str().unwrap(),
            accounts_file.path().to_str().unwrap(),
        ) {
            Ok(()) => {}
            Err(e) => println!(
                "Some error occurred loading the engine from previous dump: {}",
                e
            ),
        }

        // Compare accounts
        for entry in engine.accounts.iter() {
            let client_id = *entry.key();
            let account = entry.value();
            if let Some(account2) = engine2.accounts.get(&client_id) {
                assert_eq!(
                    *account, *account2,
                    "Account mismatch for client {}",
                    client_id
                );
            } else {
                panic!("Account for client {} not found in engine2", client_id);
            }
        }

        // Compare transactions
        for entry in engine.transaction_log.iter() {
            let tx_id = *entry.key();
            let transaction = entry.value();
            if let Some(transaction2) = engine2.transaction_log.get(&tx_id) {
                assert_eq!(
                    *transaction, *transaction2,
                    "Transaction mismatch for tx_id {}",
                    tx_id
                );
            } else {
                panic!("Transaction with tx_id {} not found in engine2", tx_id);
            }
        }
    }
}

use std::sync::Arc;
use std::thread;
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
#[test]
fn reg_test_engine_consistency_with_concurrent_processing() {
    const BUF_SIZE: usize = 1024;
    // Generate 3 temp files with consistent random transactions
    let temp_file1 = generate_random_transaction_concurrent_stream(10000, 0, 1, 10).unwrap();
    let temp_file2 = generate_random_transaction_concurrent_stream(10000, 10001, 200, 300).unwrap();
    let temp_file3 = generate_random_transaction_concurrent_stream(10000, 20001, 400, 500).unwrap();

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
    let engine1 = Engine::new();
    let engine2 = Arc::new(Engine::new());

    // Process files sequentially with engine1
    match engine1.read_and_process_transactions(File::open(temp_file1.path()).unwrap(), BUF_SIZE) {
        Ok(()) => {}
        Err(..) => {}
    }
    match engine1.read_and_process_transactions(File::open(temp_file2.path()).unwrap(), BUF_SIZE) {
        Ok(()) => {}
        Err(..) => {}
    }
    match engine1.read_and_process_transactions(File::open(temp_file3.path()).unwrap(), BUF_SIZE) {
        Ok(()) => {}
        Err(..) => {}
    }

    // Process files concurrently with engine2
    let handles = vec![
        {
            let engine2 = Arc::clone(&engine2);
            let file = File::open(temp_file1.path()).unwrap();
            thread::spawn(
                move || match engine2.read_and_process_transactions(file, BUF_SIZE) {
                    Ok(()) => {}
                    Err(..) => {}
                },
            )
        },
        {
            let engine2 = Arc::clone(&engine2);
            let file = File::open(temp_file2.path()).unwrap();
            thread::spawn(
                move || match engine2.read_and_process_transactions(file, BUF_SIZE) {
                    Ok(()) => {}
                    Err(..) => {}
                },
            )
        },
        {
            let engine2 = Arc::clone(&engine2);
            let file = File::open(temp_file3.path()).unwrap();
            thread::spawn(
                move || match engine2.read_and_process_transactions(file, BUF_SIZE) {
                    Ok(()) => {}
                    Err(..) => {}
                },
            )
        },
    ];

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    /*  debug
    writer.write_record(["client", "available", "held", "total", "locked"]).unwrap();
    writer.flush().unwrap();
    let _ = serialize_account_balances_csv(&engine1.accounts, std::io::stdout());
    writer.write_record(["client", "available", "held", "total", "locked"]).unwrap();
    writer.flush().unwrap();
    let _ = serialize_account_balances_csv(&engine2.accounts, std::io::stdout()); */

    // Compare account snapshots
    for account1_entry in engine1.accounts.iter() {
        if let Some(account2) = engine2.accounts.get(account1_entry.key()) {
            assert_eq!(
                account1_entry.value().available,
                account2.available,
                "Available balance mismatch for client {}",
                account1_entry.key()
            );
            assert_eq!(
                account1_entry.value().held,
                account2.held,
                "Held balance mismatch for client {}",
                account1_entry.key()
            );
            assert_eq!(
                account1_entry.value().total,
                account2.total,
                "Total balance mismatch for client {}",
                account1_entry.key()
            );
            assert_eq!(
                account1_entry.value().locked,
                account2.locked,
                "Locked status mismatch for client {}",
                account1_entry.key()
            );
        } else {
            panic!(
                "Account for client {} not found in engine2",
                account1_entry.key()
            );
        }
    }
}
