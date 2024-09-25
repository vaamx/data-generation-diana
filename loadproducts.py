import snowflake.connector
import csv

# Connection details
conn = snowflake.connector.connect(
    user='OPSCALEAI',
    password='Opscale2030',
    account='nvvmnod-mw08757',
    warehouse='DIANA_DATA_LAKE',
    database='DIANA_SALES_ES',
    schema='SALES'
)

# Create a cursor object
cur = conn.cursor()

# Path to your CSV file
file_path = '/home/vaamx/Opscale/Demo/Diana/processed_products.csv'

# Open the CSV file and skip the header
with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Extract data from each row
        product_name = row['PRODUCT_NAME']
        product_category_id = row['PRODUCT_CATEGORY_ID']
        price = row['PRICE']
        production_date = row['PRODUCTION_DATE']
        description = row['DESCRIPTION']

        # Check if the product already exists in the table
        cur.execute("""
            SELECT COUNT(*) FROM DIANA_SALES_ES.SALES.PRODUCTS 
            WHERE PRODUCT_NAME = %s
        """, (product_name,))
        result = cur.fetchone()

        # If the product does not exist, insert it into the table
        if result[0] == 0:
            cur.execute("""
                INSERT INTO DIANA_SALES_ES.SALES.PRODUCTS
                (PRODUCT_NAME, PRODUCT_CATEGORY_ID, PRICE, PRODUCTION_DATE, DESCRIPTION)
                VALUES (%s, %s, %s, %s, %s);
            """, (product_name, product_category_id, price, production_date, description))

# Commit changes
conn.commit()

# Close cursor and connection
cur.close()
conn.close()

print("Data loaded successfully without duplicates!")
