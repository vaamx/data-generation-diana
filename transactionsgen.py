import snowflake.connector
import pandas as pd
from faker import Faker
import random
from tqdm import tqdm
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Snowflake connection details
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='SALES'
    )

# Fetch store and product data from Snowflake
def fetch_snowflake_data(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT STORE_ID FROM DIANA_SALES_ES.STOREFRONTS.STORES;")
        stores_df = pd.DataFrame(cursor.fetchall(), columns=['STORE_ID'])

        cursor.execute("SELECT PRODUCT_ID, PRICE FROM DIANA_SALES_ES.SALES.PRODUCTS;")
        products_df = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'PRICE'])

    return stores_df['STORE_ID'].tolist(), products_df

# Generate random transactions for a year
def generate_transactions(store_ids, product_data, num_weeks=52):
    transactions = []
    transaction_id = 1
    start_date = datetime.today() - timedelta(weeks=num_weeks)

    for store_id in tqdm(store_ids, desc="Generating Transactions", unit="store"):
        num_transactions = random.randint(1, 3 * num_weeks)  # Simulating 1 to 3 transactions per week

        for _ in range(num_transactions):
            purchase_date = fake.date_between(start_date=start_date, end_date="today")
            transaction_group_id = transaction_id
            num_products = random.randint(4, len(product_data))  # Simulate purchasing between 4 and up to 10 products


            selected_products = product_data.sample(n=num_products)
            
            product_categories = {prod_id: 'staple' for prod_id in product_data['PRODUCT_ID'].tolist()}  # Simplified example

            for _, product in selected_products.iterrows():
                if product_categories[product['PRODUCT_ID']] == 'staple':
                    max_qty = 50  # Allow up to 20 items for staple goods
                else:
                    max_qty = 20  # Non-staple items have a max of 10

                quantity = random.randint(10, max_qty)  # Random quantity based on category
                total_value = product['PRICE'] * quantity

                transactions.append({
                    'TRANSACTION_ID': transaction_id,
                    'TRANSACTION_GROUP_ID': transaction_group_id,
                    'STORE_ID': store_id,
                    'PRODUCT_ID': product['PRODUCT_ID'],
                    'PURCHASE_DATE': purchase_date.strftime('%Y-%m-%d'),
                    'QUANTITY': quantity,
                    'TOTAL_VALUE': total_value
                })
                transaction_id += 1

    return transactions

def main():
    conn = get_snowflake_connection()
    try:
        store_ids, product_data = fetch_snowflake_data(conn)
        transactions = generate_transactions(store_ids, product_data)
        df_transactions = pd.DataFrame(transactions)
        df_transactions.to_csv('generated_transactions.csv', index=False)
        print(f"Generated {len(df_transactions)} transactions and saved to 'generated_transactions.csv'.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
