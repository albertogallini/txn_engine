


# Transaction Engine

A Rust-based toy payments engine that processes transactions from a CSV input, updates client accounts,
 handles disputes, resolutions, and chargebacks, and outputs the final state of accounts as a CSV.

## Overview

This project implements a transaction processing system with the following capabilities:
- Processes deposits, withdrawals, disputes, resolutions, and chargebacks.
- Manages client accounts, including available, held, and total funds.
- Handles transaction disputes for both deposits and withdrawals.
- Locks accounts upon chargeback.

### Features

- **CSV Input/Output**: Reads transactions from a CSV file and writes account summaries to stdout in CSV format.
- **Transaction Types**:
  - **Deposit**: Increases the available and total funds of an account.
  - **Withdrawal**: Decreases the available and total funds if sufficient funds are present.
  - **Dispute**: Moves disputed funds from available to held, keeping total funds constant.
  - **Resolve**: Moves funds back from held to available, ending a dispute.
  - **Chargeback**: Reverses a disputed transaction, reducing total funds and locking the account.
- **Error Handling**: Comprehensive error checks throughout transaction processing.
- **Memory Efficiency**: Processes transactions in batches to manage memory usage even with large datasets.

## Getting Started

### Prerequisites

- Rust (stable)
- Cargo, Rust's package manager

### Installation

```sh
git clone https://github.com/albertogallini/txn_engine.git
cd txn-engine
cargo build --release
```

### Usage
To process transactions:

```sh
cargo run --release -- transactions.csv > accounts.csv
```

For stress testing with generated transactions:

```sh
cargo run --release -- stress-test 10000 > accounts.csv
```

Running Tests

```sh
cargo test
```

For stress testing suite to measure time and memory conumption:

```sh
./stress-test.sh
```

## Implementation Description & Assumptions 

The transaction engine processes transactions from CSV input including deposits, withdrawals, disputes, resolutions, and chargebacks. 
It manages client accounts with available, held, and total balances while supporting batch processing for large datasets to ensure memory efficiency. 
Safe arithmetic operations prevent overflow errors, and it handles disputes for both deposits and withdrawals, with negative holding for the latter
Accounts are locked upon chargeback, and the system provides detailed error reporting. It also includes stress testing capabilities by generating random transactions for performance analysis, outputs account statuses to CSV, and leverages Rust's ownership for secure memory management.

### Project Structure

This project consists of several key components, each responsible for different aspects of transaction processing. Below is a schema of the main structs and functions and their interactions within the project.

#### Structs

- **Transaction**: Represents a financial transaction. Contains fields such as type, client, transaction ID, and amount.
- **Account**: Represents a client's account. Manages balances including available, held, and total funds.
- **Engine**: Core processing unit that handles transactions, manages accounts, and ensures integrity and correctness of operations.

#### Functions

- **main**: Entry point of the application. Parses arguments, distinguishes between normal processing and stress testing, and initiates transaction processing.
- **generate_random_transactions**: Creates a CSV file with randomly generated transactions for stress testing purposes.
- **process_normal**: Processes transactions from a provided CSV file and updates account states accordingly.
- **process_stress_test**: Handles stress testing by processing a large number of generated transactions and measuring performance metrics.
- **read_and_process_transactions**: Reads transactions from a CSV file and dispatches them for processing by the engine.
- **output_results**: Outputs the final state of all accounts to a CSV file after processing is complete.

#### Key Methods in Engine

- **new**: Initializes a new engine instance.
- **check_transaction_semantic**: Verifies the semantic validity of transactions, ensuring they adhere to business rules.
- **safe_add / safe_sub**: Performs arithmetic operations safely, preventing overflow errors.
- **process_transaction**: Dispatches a transaction to the appropriate processing function based on its type.
- **size_of**: Estimates the memory usage of the engine and its data structures.

#### Error Handling

The system includes comprehensive error handling with specific error messages for various conditions like insufficient funds, account not found, and transaction disputes.
The system handles the following error conditions:

- **ERROR_DIFFERENT_CLIENT**: If a dispute or resolve is attempted on a transaction from a different client.
- **ERROR_NO_AMOUNT**: If a transaction does not have an amount.
- **ERROR_DEPOSIT_AMOUNT**: If the amount of a deposit is not greater than 0.
- **ERROR_WITHDRAWAL_AMOUNT**: If the amount of a withdrawal is not greater than 0.
- **ERROR_TX_REPEATED**: If a transaction id already processed in this session - cannot be repeated.
- **ERROR_INSUFFICIENT_FUNDS**: If a client does not have enough available funds for a withdrawal.
- **ERROR_ACCOUNT_NOT_FOUND**: If an account is not found for a transaction.
- **ERROR_TX_NOT_FOUND**: If a transaction is not found for a dispute or resolve operation.
- **ERROR_ADDITION_OVERFLOW**: If an addition operation would result in an overflow.
- **ERROR_SUBTRACTION_OVERFLOW**: If a subtraction operation would result in an overflow.
- **ERROR_ACCOUNT_LOCKED**: If an account is locked.
- **ERROR_TX_ALREADY_DISPUTED**: If a dispute is attempted on an already disputed transaction.
- **ERROR_TX_NOT_DISPUTED**: If a resolve or chargeback is attempted on a non-disputed transaction.


#### Memory Efficiency
The engine is designed to be memory efficient, processing transactions in batches (through buffering the input csv stream) and estimating memory usage to ensure scalability even with large datasets.

#### Concurrency Management
In spite of `main.rs` implementing a single process that reads sequentially from an input CSV stream, the internal `Engine` is designed to support concurrent input transaction streams. Incorporating `DashMap` into the `Engine` struct for managing `accounts` and `transaction_log` provides a concurrent, thread-safe hash map implementation that significantly enhances our system's performance and scalability. <u>By allowing multiple threads to read or write to different entries simultaneously without explicit locking, `DashMap` reduces lock contention: Instead of locking the entire map or individual entries, `DashMap` uses fine-grained locking internally, reducing contention when many threads are accessing different parts of the data map. It improves memory efficiency, and simplifies our codebase, making it easier to manage concurrent operations across potentially thousands of client transactions</u>. This choice supports the goal of creating a high-throughput, low-latency transaction processing system that can scale with demand, all while maintaining code maintainability.

#### Generalizing Disputes:
- Deposits: When disputing a deposit, you would move the disputed amount from available to held. This keeps the total the same since you're just reallocating the funds.
- Withdrawals: When disputing a withdrawal, the process is similar but with a twist: the amount held would indeed be negative because it represents money that was taken out (withdrawn) from the account but is now under dispute. Holding a negative amount means you're reserving the possibility that this withdrawal could be reversed, effectively increasing the account's available balance by this negative (or positive in terms of adding back) amount while the dispute is unresolved.

For a Disputed Deposit:
- Available: Decreases by the disputed amount.
- Held: Increases by the disputed amount.
- Total: Remains unchanged.

For a Disputed Withdrawal:
- Available: Increases by the disputed amount (since you're essentially holding back the withdrawal).
- Held: Decreases by the disputed amount (negative held).
- Total: Remains unchanged because you're just moving what was taken out back into a different category (held).

Allowing for negative held funds for withdrawals means that if the dispute results in a resolve, you'd decrease the held (which is negative) and increase available, effectively returning the withdrawn money back into available funds.
For a chargeback, you'd reduce the total by the (negative) held amount, which means adding the disputed withdrawal back to the account, but since the account is then locked, this might require special handling for accounting or regulatory purposes.

Implementation: see `fn check_transaction_semantic` in `./src/engine.rs`


## Stress Test script:

The stress_test.sh script runs the program with increasing numbers of transactions and measures execution time and memory consumption. 
The `generate_random_transactions` function is used by the `stress-test` mode of the `txn_engine` to generate random transactions in the CSV format. It takes in two parameters: the number of transactions to generate and the output file path.

The function works as follows:
- It opens the output file using the provided path.
- It writes the header line to the file with the column names.
- For each transaction, it randomly selects a type (deposit, withdrawal, dispute, resolve, chargeback) and a client ID between 1 and 1000.
- For deposit and withdrawal transactions, it generates a random amount between 0.0 and 10000.0.
- It writes each transaction line to the file.
- Then it loops from 100 to 1000100 transactions in steps of 100 and measures the time and memory consumption of the program. The output is written to stress_test_results.txt in the format `Transactions Count, Time, Process Memory (MB), Engine Memory (MB)`.

Note: The `generate_random_transactions` function is not meant to mimic real-world transactions since it generates random transactions without any ordering or dependencies. This results in a higher number of error conditions compared to real-world use cases. But it is good enough to see how the system resources are used increasing the size of the input.

Example of output on Mac-Book M3 24 Gb :
```
Transactions Count, Time          , Process Memory (MB), Engine Memory (MB)
100               , 428.083µs     , 2.094              , 0.001             
100100            , 173.342542ms  , 26.203             , 0.347             
200100            , 381.261166ms  , 0.000              , 0.356             
300100            , 574.925708ms  , 0.000              , 0.357             
400100            , 766.088583ms  , 0.000              , 0.357             
500100            , 980.495375ms  , 194.016            , 0.356             
600100            , 1.155201959s  , 109.203            , 0.357             
700100            , 1.365203708s  , 0.000              , 0.357             
800100            , 1.568585208s  , 147.188            , 0.357             
900100            , 1.785412333s  , 0.000              , 0.357             
1000100           , 1.940334541s  , 186.625            , 0.357              
         
```
So overall performance of txn_engine, on the aformenthioned assumption, on this machine is `~500.000 transactions/s`  with a avg `~[2500 - 5000] transation/Mb` memory impact on the user account/transaction log storage.

NOTE: memeory measure are very approximate see `pub fn size_of(&self) -> usize` of `Engine` struct. I did not used that numbers as do not look correct for above performance measure. I used the Process Memory as upperbound. Process Memory is not always reliaable as it depednd on how th OS allocate/deallocate memory for the process and does not reflect directly the real data structures memory usage.


