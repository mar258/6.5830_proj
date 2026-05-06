import csv
import random

# Configuration
NUM_REGIONS = 50          # T0
NUM_USERS = 40_000       # T3
NUM_ORDERS = 1_000_000   # T1
NUM_LOGS = 1_000_000     # T2

def generate_regions():
    print("Generating regions (T0)...")
    with open('regions.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['region_id', 'region_name_hash'])
        for i in range(1, NUM_REGIONS + 1):
            writer.writerow([i, f"Region_{i}"])

def generate_users():
    print("Generating users (T3)...")
    with open('users.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['user_id', 'region_id', 'account_balance'])
        for i in range(1, NUM_USERS + 1):
            # Assign user to a random region
            writer.writerow([i, random.randint(1, NUM_REGIONS), round(random.uniform(0, 1000), 2)])

def generate_orders():
    print("Generating orders (T1)...")
    with open('orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'user_id', 'order_amount'])
        for i in range(1, NUM_ORDERS + 1):
            # Random user makes an order
            writer.writerow([i, random.randint(1, NUM_USERS), round(random.uniform(10, 500), 2)])

def generate_web_logs():
    print("Generating web logs (T2)...")
    with open('web_logs.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['log_id', 'user_id', 'action_type'])
        actions = [1, 2, 3, 4] # e.g., 1=view, 2=click, 3=cart, 4=purchase
        for i in range(1, NUM_LOGS + 1):
            writer.writerow([i, random.randint(1, NUM_USERS), random.choice(actions)])

if __name__ == "__main__":
    generate_regions()
    generate_users()
    generate_orders()
    generate_web_logs()
    print("Dataset generation complete.")