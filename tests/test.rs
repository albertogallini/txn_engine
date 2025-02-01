
use txn_engine::engine::read_and_process_transactions;
use txn_engine::engine::Engine; // Note the path adjustment if needed
use rust_decimal::Decimal;
use std::str::FromStr;



#[test]
fn test_basic() {
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
    assert_eq!(engine.accounts[&1].total, engine.accounts[&1].available );
    assert_eq!(engine.accounts[&2].total, engine.accounts[&2].available);
    assert_eq!(engine.accounts[&1].held, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&2].held, Decimal::from_str("0").unwrap());
}


#[test]
fn test_disputed() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_disputed.csv";
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }
    print!("Accounts: {:#?}", engine.accounts);
    assert_eq!(engine.accounts.len(),6);
    assert_eq!(engine.accounts[&3].total, Decimal::from_str("100").unwrap());
    assert_eq!(engine.accounts[&5].total, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&4].total, Decimal::from_str("0").unwrap() );
    
    assert!(!engine.accounts[&3].locked);
    assert!(engine.accounts[&5].locked );
    assert!(engine.accounts[&4].locked );

    assert_eq!(engine.accounts[&10].total, Decimal::from_str("80").unwrap());
    assert_eq!(engine.accounts[&20].total, Decimal::from_str("80").unwrap());
    assert_eq!(engine.accounts[&30].total, Decimal::from_str("120").unwrap() );

    assert_eq!(engine.accounts[&10].held, Decimal::from_str("-20").unwrap());
    assert_eq!(engine.accounts[&20].held, Decimal::from_str("0").unwrap());
    assert_eq!(engine.accounts[&30].held, Decimal::from_str("20").unwrap() );
}


/*
Tests the handling of erroneous transactions from a CSV file.

type       ,client,tx   ,amount

deposit    ,6     ,9    ,0.0000
withdrawal ,6     ,10   ,-5.0000       # Negative amount, should fail
deposit    ,6     ,11   ,79228162514264337593543950330  # Large amount
deposit    ,6     ,12   ,5000.0000     # Addition overflowed
withdrawal ,6     ,13   ,              # Empty amount --> fail

deposit    ,7     ,14   ,              # Empty amount --> fail
deposit    ,7     ,15   ,10.0              
deposit    ,7     ,15   ,10.0          # Duplicate tx                                       !!!!!
dispute    ,7     ,16   ,              # Dispute on non-existent or invalid tx

resolve    ,6     ,9999 ,              # Resolve on non-existent tx

chargeback ,7     ,16   ,              # Chargeback on non-existent tx
*/

#[test]
fn test_errors() {
    let mut engine = Engine::default();
    let input_path = "tests/transactions_errors.csv";
    match read_and_process_transactions(&mut engine, input_path) {
        Ok(()) => println!("Transactions processed successfully"),
        Err(e) => println!(" Some error occurred while processing transactions: {}", e),
    }
    assert_eq!(engine.accounts.len(), 2);
}
