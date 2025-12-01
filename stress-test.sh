#!/bin/bash

# Build the release version of the program
cargo build --release

# Output file for storing results
output_file="stress_test_results.txt"

# Clear or create the output file
> "$output_file"

# Loop through transaction counts using seq for the range
for i in $(seq 100 100000 2000100); do
    # Format the number as an integer
    formatted_i=$(printf "%.0f" $i)
    
    echo "Running stress test $1 for $formatted_i transactions..."

    # Capture the output of the program in a variable
    output=$(./target/release/txn_engine $1 stress-test $formatted_i 2>&1)
    echo "$output" > stress_test_results_debug_output_$formatted_i.txt  # Save the output to a debug file

    # Grep for memory consumption
    memory=$(echo "$output" | grep "Memory consumption delta" | awk '{print $4}')

    # Grep for engine memory consumption
    engine_memory=$(echo "$output" | grep "Engine Memory size" | awk '{print $4}')

    # Grep for elapsed time
    time=$(echo "$output" | grep "Elapsed time" | awk '{print $3, $4}')

    
    # Check if the output contains valid results
    if [[ -n "$memory" && -n "$time" && -n "$engine_memory" ]]; then
        # Append results to the output file
        echo "$formatted_i $time $memory $engine_memory" >> "$output_file"
    else
        echo "Failed to capture results for $formatted_i transactions."
    fi
done

# Print the series from the output file with aligned columns
awk 'BEGIN { 
    printf "%-20s %-20s %-20s %-20s\n", "Transactions Count", "Time", "Process Memory (MB)", "Engine Memory (MB)"
} { 
    printf "%-20s %-20s %-20s %-20s\n", $1, $2, $3, $4, $5
}' "$output_file"
