import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def upload_to_snowflake():
    # Connection parameters
    conn_params = {
        'user': 'OPSCALEAI',
        'password': 'Opscale2030',
        'account': 'nvvmnod-mw08757',
        'warehouse': 'DIANA_DATA_LAKE',
        'database': 'DIANA_SALES_ES',
        'schema': 'STOREFRONTS'
    }

    # Connect to Snowflake
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()

    try:
        # Load CSV into DataFrame
        df = pd.read_csv('/home/vaamx/Opscale/Demo/Diana/generated_stores.csv')
        
        # Define the column names as per the Snowflake table structure
        # Ensure this list matches exactly with what's in Snowflake
        df.columns = ['STORE_NAME', 'STORE_DESCRIPTION', 'ADDRESS', 'CONTACT_NAME', 'CONTACT_EMAIL', 'CONTACT_PHONE', 'REGION_ID']
        
        # Use the write_pandas function to handle the DataFrame to Snowflake table transfer
        success, nchunks, nrows, _ = write_pandas(conn, df, 'STORES')
        
        print(f"Successfully loaded {nrows} rows to the STORES table in Snowflake.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    upload_to_snowflake()