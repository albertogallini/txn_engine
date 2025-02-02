use rust_decimal::Decimal;
use std::str::FromStr;
use txn_engine::datastr::transaction::TransactionProcessingError;
use txn_engine::engine::read_and_process_transactions;
use txn_engine::engine::Engine; // Note the path adjustment if needed

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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => panic!("Error processing transactions: {}", e),
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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => panic!("Error processing transactions: {}", e),
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
fn unit_test_deposit_and_dispute_resolve() {
    let mut temp_file = NamedTempFile::new().unwrap();
    let csv_content = r#"type,client,tx,amount,\n
                                deposit,1,1,10.0000,\n
                                dispute,1,1,,\n
                                resolve,1,1,,\n"#;

    write!(temp_file, "{}", csv_content).unwrap();
    let input_path = temp_file.path().to_str().unwrap();

    let mut engine = Engine::default();
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => panic!("Error processing transactions: {}", e),
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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => panic!("Error processing transactions: {}", e),
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
        account.locked,
        true,
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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => panic!("Error processing transactions: {}", e),
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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => panic!("Should not process withdrawal when insufficient funds"),
        Err(e) => {
            assert!(
                e.to_string().contains("Insufficient funds"),
                "Expected insufficient funds error"
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
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => panic!("Should not process withdrawal from zero balance"),
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
fn test_from_csv_file_basic() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_basic.csv";
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }
    print!("Accounts: {:#?}", engine.accounts);
    assert_eq!(engine.accounts.len(), 2);
    assert_eq!(engine.accounts[&1].total, Decimal::from_str("10").unwrap());
    assert_eq!(engine.accounts[&2].total, Decimal::from_str("5").unwrap());
    assert_eq!(engine.accounts[&1].total, engine.accounts[&1].available);
    assert_eq!(engine.accounts[&2].total, engine.accounts[&2].available);
    assert_eq!(engine.accounts[&1].held, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&2].held, Decimal::from_str("0").unwrap());
}

#[test]
fn test_from_csv_file_disputed() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_disputed.csv";
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }
    print!("Accounts: {:#?}", engine.accounts);
    assert_eq!(engine.accounts.len(), 6);
    assert_eq!(engine.accounts[&3].total, Decimal::from_str("100").unwrap());
    assert_eq!(engine.accounts[&5].total, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&4].total, Decimal::from_str("0").unwrap());

    assert!(!engine.accounts[&3].locked);
    assert!(engine.accounts[&5].locked);
    assert!(engine.accounts[&4].locked);

    assert_eq!(engine.accounts[&10].total, Decimal::from_str("80").unwrap());
    assert_eq!(engine.accounts[&20].total, Decimal::from_str("80").unwrap());
    assert_eq!(
        engine.accounts[&30].total,
        Decimal::from_str("120").unwrap()
    );

    assert_eq!(engine.accounts[&10].held, Decimal::from_str("-20").unwrap());
    assert_eq!(engine.accounts[&20].held, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&30].held, Decimal::from_str("20").unwrap());
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
    match read_and_process_transactions(&mut engine, input_path) {
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
    match read_and_process_transactions(&mut engine, input_path) {
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
