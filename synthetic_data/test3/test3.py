import csv
import random
import time

# The exact row counts requested
TABLE_SIZES = [1_500_000, 150_000, 100_000, 25]

def generate_skewed_chain():
    total_rows = sum(TABLE_SIZES)
    print(f"Generating 5 tables with a total of {total_rows:,} rows...\n")
    start_time = time.time()

    for t, size in enumerate(TABLE_SIZES):
        filename = f"table_{t}.csv"
        print(f"[{t+1}/5] Generating {filename} ({size:,} rows)...")
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Table 0 is the base. Subsequent tables link to the previous one.
            if t == 0:
                headers = ['id', 'data_val']
            else:
                headers = ['id', f't{t-1}_id', 'data_val']
            writer.writerow(headers)
            
            # Batch data generation in memory for fast disk writing
            batch = []
            for i in range(1, size + 1):
                data_val = f"payload_t{t}_row{i}"
                
                if t == 0:
                    batch.append([i, data_val])
                else:
                    # Randomly link to an ID from the previous table
                    prev_table_size = TABLE_SIZES[t-1]
                    fk_id = random.randint(1, prev_table_size)
                    batch.append([i, fk_id, data_val])
                
                # Write in chunks of 100,000 to balance memory and I/O
                if i % 100_000 == 0:
                    writer.writerows(batch)
                    batch = []
            
            # Write any remaining rows
            if batch:
                writer.writerows(batch)
                
        print(f"  ✓ Finished {filename}")

    elapsed = time.time() - start_time
    print(f"\nSuccessfully generated {total_rows:,} total rows in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    # Seed the random number generator for reproducible benchmark data
    random.seed(42)
    generate_skewed_chain()