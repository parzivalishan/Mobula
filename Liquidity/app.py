import csv
import json
import re
import os
import sys
from collections import defaultdict
from datetime import datetime

def analyze_liquidity_by_transfer(data: str, contract_address: str):
    """
    Analyzes total token liquidity of a smart contract by tracking 'Transfer' events.
    Calculates hourly net changes and maintains a running cumulative total.

    Args:
        data (str): The raw log data as a multi-line string.
        contract_address (str): The Ethereum address of the smart contract to track.
    """
    lines = data.strip().splitlines()
    hourly_net_change = defaultdict(int)
    normalized_contract_address = contract_address.lower()

    # This Regex reliably captures the 5 main columns from your CSV data.
    # It correctly handles the quoted JSON string at the end.
    line_regex = re.compile(r'^([^,]+),([^,]+),([^,]+),([^,]+),"(.+)"$')

    for line in lines:
        match = line_regex.match(line)
        
        # Skip any line that doesn't match the expected format
        if not match:
            continue

        timestamp_str, _, _, event_name, raw_json_data = match.groups()

        # We only care about 'Transfer' events for tracking the contract balance.
        if event_name == "Transfer":
            try:
                # Parse the timestamp and format it to an hourly key.
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                hour_key = timestamp.strftime('%Y-%m-%d %H:00:00')
                
                # The JSON within the log is escaped with double-quotes ("").
                # This replaces them with a single quote (") to make it valid JSON.
                clean_json_data = raw_json_data.replace('""', '"')
                event_data = json.loads(clean_json_data)

                from_addr = event_data.get("from", "").lower()
                to_addr = event_data.get("to", "").lower()
                value_str = event_data.get("value")

                if not value_str:
                    continue # Skip if there's no value

                value = int(value_str)

                # INFLOW: Tokens are transferred TO the contract, increasing liquidity.
                if to_addr == normalized_contract_address:
                    hourly_net_change[hour_key] += value

                # OUTFLOW: Tokens are transferred FROM the contract, decreasing liquidity.
                elif from_addr == normalized_contract_address:
                    hourly_net_change[hour_key] -= value
            
            except (json.JSONDecodeError, ValueError) as e:
                # Log lines that fail to parse for debugging but continue execution.
                print(f"Skipping malformed line: {line}\nError: {e}", file=sys.stderr)
                continue

    # --- Display the Results ---
    print("--- Hourly Liquidity Breakdown (Based on Transfers Only) ---")
    print(f"{'Hour':<20} | {'Net Change in Liquidity':<30} | {'Cumulative Liquidity'}")
    print("-" * 85)

    running_total_liquidity = 0
    
    # Sort the hours chronologically before printing.
    sorted_hours = sorted(hourly_net_change.keys())
    
    for hour in sorted_hours:
        net_change = hourly_net_change[hour]
        running_total_liquidity += net_change
        print(f"{hour:<20} | {net_change:<30} | {running_total_liquidity}")

    print("-" * 85)
    print(f"\nFinal Calculated Cumulative Liquidity: {running_total_liquidity}")


def main():
    """
    Main execution block to read the file and run the analysis.
    """
    # Define the contract address to analyze.
    contract_address = "0x43C3EBaFdF32909aC60E80ee34aE46637E743d65"
    
    # The script expects 'history.csv' to be in the same directory.
    file_path = os.path.join(os.path.dirname(__file__), 'history.csv')

    try:
        print(f"Reading data from: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            log_data = f.read()
        
        if not log_data.strip():
            print("Error: 'history.csv' is empty. No data to analyze.", file=sys.stderr)
            sys.exit(1)
        
        # Call the analysis function with the file's content.
        analyze_liquidity_by_transfer(log_data, contract_address)
        
    except FileNotFoundError:
        print(f"Error: Could not find the file at '{file_path}'.", file=sys.stderr)
        print("Please make sure 'history.csv' exists in the same directory as the script.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
