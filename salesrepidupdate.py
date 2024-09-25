import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='STOREFRONTS'
)

# Create a cursor object
cur = conn.cursor()

try:
    # Fetch sales rep IDs and store IDs based on region matches
    cur.execute("""
        SELECT t.STORE_ID, s.SALESREP_ID
        FROM STOREFRONTS.SALESPERSONNEL s
        JOIN STOREFRONTS.STORES t ON s.REGION_ID = t.REGION_ID
    """)
    store_salesrep_mapping = cur.fetchall()

    # Update transactions table
    for store_id, salesrep_id in store_salesrep_mapping:
        cur.execute("""
            UPDATE SALES.TRANSACTIONS
            SET SALESREP_ID = %s
            WHERE STORE_ID = %s
        """, (salesrep_id, store_id))

    # Commit the transaction
    conn.commit()

except Exception as e:
    print(f"An error occurred: {e}")
    conn.rollback()

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()

