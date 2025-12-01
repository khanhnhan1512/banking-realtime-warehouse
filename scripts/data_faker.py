import time
import random
import psycopg2
import argparse
import sys
import os
from decimal import Decimal, ROUND_DOWN
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

# Configurable parameters
NUM_CUSTOMERS = 5
NUM_ACCOUNTS = 5 
NUM_TRANSACTIONS = 5

MAX_TRANSACTION_AMOUNT = 10_000_000.00
CURRENCY = 'VND'

INIT_BALANCE_MIN = Decimal('1_000_000.00')
INIT_BALANCE_MAX = Decimal('50_000_000.00')

# Loop
DEFAULT_LOOP = True
LOOP_DELAY = 1

# CLI
parser = argparse.ArgumentParser(description='Data Faker for Banking Realtime Warehouse')
parser.add_argument("--once", action='store_true', help="Run the script only once") # when --once is provided, args.once = True
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# faker instance
fake = Faker()

def random_money(min_val, max_val):
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal('0.01'), rounding=ROUND_DOWN)

# Database connection
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("Connected to the database successfully.")
except Exception as e:
    print(f"Error connecting to database: {e}")
    sys.exit(1)

# Generation logic
def generate_data():
    new_customer_ids = []
    new_account_ids = []

    print(f"--- 1. Creating {NUM_CUSTOMERS} Customers ---")
    # Customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        phone = fake.unique.phone_number()

        cur.execute(
            """
            INSERT INTO customers(first_name, last_name, email, phone)
            VALUES (%s, %s, %s, %s) RETURNING id
            """,
            (first_name, last_name, email, phone)
        )
        customer_id = cur.fetchone()[0]
        new_customer_ids.append(customer_id)

    print(f"--- 2. Creating {NUM_ACCOUNTS} Accounts ---")
    # Accounts
    for _ in range(NUM_ACCOUNTS):
        account_type = random.choice(['SAVINGS', 'CHECKING', 'CREDIT'])
        # Đảm bảo bạn đã import random_money và các biến global
        init_balance = random_money(INIT_BALANCE_MIN, INIT_BALANCE_MAX) 

        cur.execute(
            """
            INSERT INTO accounts(account_type, balance, currency)
            VALUES (%s, %s, %s) RETURNING id
            """,
            (account_type, init_balance, CURRENCY)
        )
        account_id = cur.fetchone()[0]
        new_account_ids.append(account_id)
    
    print("--- 3. Mapping Customers to Accounts ---")
    # Customer-Account Mapping
    for acc_id in new_account_ids:
        primary_owner_id = random.choice(new_customer_ids)
        cur.execute(
            """
            INSERT INTO customers_accounts(customer_id, account_id, role)
            VALUES (%s, %s, 'PRIMARY_OWNER')
            """,
            (primary_owner_id, acc_id)
        )

        # We will set a 20% chance of having a secondary owner
        if random.random() < 0.2:
            available_co_owners = [cid for cid in new_customer_ids if cid != primary_owner_id]

            if available_co_owners:
                co_owner_id = random.choice(available_co_owners)
                cur.execute(
                    """
                    INSERT INTO customers_accounts(customer_id, account_id, role)
                    VALUES (%s, %s, 'CO_OWNER')
                    """,
                    (co_owner_id, acc_id)
                )
    
    print(f"--- 4. Generating {NUM_TRANSACTIONS} Transactions ---")
    # Transactions 
    # get some existing customer ids to increase realism
    cur.execute("SELECT id FROM accounts ORDER BY RANDOM() LIMIT 20")
    old_account_ids = [row[0] for row in cur.fetchall()]

    all_account_ids = new_account_ids + old_account_ids
    trans_type_options = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER']
    
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(all_account_ids)
        txn_type = random.choice(trans_type_options)
        amount = random_money(Decimal('10000.00'), Decimal(str(MAX_TRANSACTION_AMOUNT)))
        description = fake.sentence(nb_words=5)

        if txn_type == 'DEPOSIT':
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, account_id))
            cur.execute(
                """
                INSERT INTO transactions(account_id, transaction_type, amount, status, description)
                VALUES (%s, %s, %s, 'SUCCESS', %s)
                """,
                (account_id, 'DEPOSIT', amount, description)
            )
        
        elif txn_type == 'WITHDRAWAL':
            # Check balance
            cur.execute("SELECT balance FROM accounts WHERE id = %s", (account_id,))
            current_balance = cur.fetchone()[0]

            if current_balance >= amount:
                cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
                cur.execute(
                    """
                    INSERT INTO transactions(account_id, transaction_type, amount, status, description)
                    VALUES (%s, %s, %s, 'SUCCESS', %s)
                    """,
                    (account_id, 'WITHDRAWAL', amount, description)
                )
            else:
                cur.execute(
                    """
                    INSERT INTO transactions(account_id, transaction_type, amount, status, description)
                    VALUES (%s, %s, %s, 'FAILED', %s)
                    """,
                    (account_id, 'WITHDRAWAL', amount, description + " (Insufficient Funds)")
                )

        elif txn_type == 'TRANSFER':
            recipients = [aid for aid in new_account_ids if aid != account_id]
            if not recipients:
                continue

            related_account = random.choice(recipients)
        
            cur.execute("SELECT balance FROM accounts WHERE id = %s", (account_id,))
            sender_balance = cur.fetchone()[0]

            if sender_balance < amount:
                cur.execute(
                    """
                    INSERT INTO transactions(account_id, transaction_type, amount, related_account, status, description)
                    VALUES (%s, 'TRANSFER_OUT', %s, %s, 'FAILED', %s)
                    """,
                    (account_id, amount, related_account, "Transfer Failed: Insufficient funds")
                )
            else:
                cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, account_id))
                cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, related_account))

                cur.execute(
                    """
                    INSERT INTO transactions(account_id, transaction_type, amount, related_account, status, description)
                    VALUES (%s, 'TRANSFER_OUT', %s, %s, 'SUCCESS', %s)
                    """,
                    (account_id, amount, related_account, f"Sent to {related_account}")
                )
            
                cur.execute(
                    """
                    INSERT INTO transactions(account_id, transaction_type, amount, related_account, status, description)
                    VALUES (%s, 'TRANSFER_IN', %s, %s, 'SUCCESS', %s)
                    """,
                    (related_account, amount, account_id, f"Received from {account_id}")
                )
    
    print(f"✅ Generated batch: {NUM_CUSTOMERS} customers, {NUM_ACCOUNTS} accounts, {NUM_TRANSACTIONS} txns.")
        
try:
    print("Starting data generation...")
    iteration = 0
    while True:
        iteration += 1
        print(f"Iteration {iteration}...")
        generate_data()
        if not LOOP:
            break
        time.sleep(LOOP_DELAY)
except KeyboardInterrupt:
    print("Data generation stopped by user.")
except psycopg2.Error as db_error:
    print(f"DATABASE ERROR: {db_error}")
except Exception as e:
    print(f"\PYTHON SCRIPT ERROR: {e}")
    import traceback
    traceback.print_exc()
finally:
    if conn:
        cur.close()
        conn.close()
    print("Database connection closed.")
    sys.exit(0)