import snowflake.connector
from faker import Faker
import pandas as pd
from tqdm import tqdm  # Progress bar

# Initialize Faker for generating random names and contacts
fake = Faker()

# Function to connect to Snowflake
def get_connection():
    return snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='STOREFRONTS'
    )

# Function to fetch region ids
def fetch_region_ids(conn):
    query = "SELECT REGION_ID FROM REGION;"
    df = pd.read_sql(query, conn)
    return df['REGION_ID'].tolist()

# Function to generate sales personnel data
def generate_sales_personnel(region_ids):
    sales_personnel = []
    for region_id in region_ids:
        for _ in range(2):  # Generate two sales people per region
            person = {
                'SALESREP_NAME': fake.name(),
                'SALESREP_EMAIL': fake.email(),
                'SALESREP_PHONE': f"+503 {fake.msisdn()[3:]}",
                'REGION_ID': region_id
            }
            sales_personnel.append(person)
    return sales_personnel

# Function to insert sales personnel data into Snowflake
def insert_sales_personnel(conn, sales_personnel):
    query = """
    INSERT INTO SALESPERSONNEL (SALESREP_NAME, SALESREP_EMAIL, SALESREP_PHONE, REGION_ID)
    VALUES (%s, %s, %s, %s);
    """
    cursor = conn.cursor()
    for person in tqdm(sales_personnel, desc="Inserting Sales Personnel"):
        cursor.execute(query, list(person.values()))
    conn.commit()
    print("Sales personnel data has been successfully uploaded.")

# Main function
def main():
    conn = get_connection()
    try:
        print("Fetching region IDs...")
        region_ids = fetch_region_ids(conn)
        print("Generating sales personnel data...")
        sales_personnel = generate_sales_personnel(region_ids)
        print("Inserting sales personnel data into Snowflake...")
        insert_sales_personnel(conn, sales_personnel)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
