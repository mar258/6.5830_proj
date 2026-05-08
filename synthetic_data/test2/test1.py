import csv
import random
import time

# Configuration
NUM_TABLES = 7
NUM_ROWS = 150_000

def generate_chain_tables():
    print(f"Generating {NUM_TABLES} tables with {NUM_ROWS:,} rows each...")
    start_time = time.time()

    for t in range(NUM_TABLES):
        filename = f"table_{t}.csv"
        
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Define headers for a chain join schema
            if t == 0:
                headers = ['id', 'data_val']
            else:
                headers = ['id', f't{t-1}_id', 'data_val']
            writer.writerow(headers)
            
            # Batch data generation in memory for faster disk writing
            batch = []
            for i in range(1, NUM_ROWS + 1):
                # Simple string payload to simulate row size
                data_val = f"payload_table{t}_row{i}"
                
                if t == 0:
                    batch.append([i, data_val])
                else:
                    # Randomly link to an ID from the previous table
                    fk_id = random.randint(1, NUM_ROWS)
                    batch.append([i, fk_id, data_val])
                
                # Write in chunks of 50,000 to balance memory and I/O
                if i % 50_000 == 0:
                    writer.writerows(batch)
                    batch = []
            
            # Write any remaining rows
            if batch:
                writer.writerows(batch)
                
        print(f"  ✓ Created {filename}")

    elapsed = time.time() - start_time
    print(f"\nSuccessfully generated 1,050,000 total rows in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    # Seed the random number generator for reproducible benchmark data
    random.seed(42)
    generate_chain_tables()