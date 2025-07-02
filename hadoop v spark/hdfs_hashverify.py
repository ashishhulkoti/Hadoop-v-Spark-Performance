import subprocess
import sys
import os
from time import time

# Function to check if records in the file are sorted
def verify_records(file_path, debug):
    record_size = 16  # size of each record
    hash_size = 10    # size of the hash within each record
    read_records = 0
    sorted_records = True
    previous_hash = None
    
    # Calculate total records based on file size
    total_records = os.path.getsize(file_path) // record_size
    
    start_time = time()
    
    # Open file and start reading records
    with open(file_path, "rb") as file:
        while True:
            record = file.read(record_size)
            if not record:
                break
            read_records += 1
            
            # Extract hash value
            current_hash = record[:hash_size]
            
            # Compare with previous hash to check sort order
            if previous_hash and current_hash < previous_hash:
                sorted_records = False
                break
            
            previous_hash = current_hash
            
            # Output debug info if the debug flag is set
            if debug:
                progress = (read_records / total_records) * 100
                elapsed_time = time() - start_time
                eta = elapsed_time / progress * (100 - progress) if progress != 0 else 0
                speed = read_records * record_size / (elapsed_time if elapsed_time > 0 else 1) / (1024**2)
                print(f"[{read_records}] [VERIFY]: {progress:.2f}% completed, ETA {eta:.1f} seconds, {read_records} read, {speed:.1f} MB/sec")
    
    # Final output
    elapsed_time = time() - start_time
    speed = total_records * record_size / elapsed_time / (1024**2)
    if sorted_records:
        print(f"Read {total_records * record_size} bytes and found all records are sorted.")
    else:
        print(f"Records are not sorted. Error at record {read_records}.")
    
    # Print out final speed and elapsed time
    if debug:
        print(f"Total time: {elapsed_time:.2f} seconds, Average speed: {speed:.1f} MB/sec")

# Script execution starts here
if __name__ == "__main__":
    # Check if file path is given as a command line argument
    if len(sys.argv) < 2:
        print("Usage: python record_sort_checker.py <file_path> [-d <true/false>]")
        sys.exit(1)
    
    # Fetch the file path
    file_name = sys.argv[1]
    # Check if debug flag is set in command line arguments
    debug_flag = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else False
    
    # Call the verify function
    verify_records(file_name, debug_flag)