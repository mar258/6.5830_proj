import csv
import random
import time

# Configuration for the 1k x 10k test
NUM_OUTER = 1_000   # eval_t0
NUM_INNER = 10_000  # eval_t1

def generate_outer_inner_tables():
    print(f"Generating outer table (eval_t0) with {NUM_OUTER:,} rows...")
    start_time = time.time()
    
    # Generate eval_t0 (Outer / Left side)
    with open('eval_t0.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'outer_data'])
        
        batch = []
        for i in range(1, NUM_OUTER + 1):
            batch.append([i, f"outer_payload_{i}"])
        writer.writerows(batch)

    print(f"Generating inner table (eval_t1) with {NUM_INNER:,} rows...")
    
    # Generate eval_t1 (Inner / Right side)
    with open('eval_t1.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 't0_id', 'inner_data'])
        
        batch = []
        for i in range(1, NUM_INNER + 1):
            # Randomly link to an ID from the outer table to simulate an equi-join
            t0_id = random.randint(1, NUM_OUTER)
            batch.append([i, t0_id, f"inner_payload_{i}"])
            
            # Write in chunks if we ever scale this up
            if i % 5_000 == 0:
                writer.writerows(batch)
                batch = []
                
        if batch:
            writer.writerows(batch)

    elapsed = time.time() - start_time
    print(f"\nSuccessfully generated {NUM_OUTER + NUM_INNER:,} total rows in {elapsed:.3f} seconds.")

if __name__ == "__main__":
    # Seed the random number generator for reproducible benchmark data
    random.seed(42)
    generate_outer_inner_tables()