import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tqdm import tqdm

def upload_to_snowflake():
    # Connection parameters
    conn_params = {
        'user': 'OPSCALEAI',
        'password': 'Opscale2030',
        'account': 'nvvmnod-mw08757',
        'warehouse': 'DIANA_DATA_LAKE',
        'database': 'DIANA_SALES_ES',
        'schema': 'SALES'
    }

    # Connect to Snowflake
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()

    try:
        # Load CSV into DataFrame with a progress bar
        csv_file = '/home/vaamx/Opscale/Demo/Diana/data_generation/generated_transactions.csv'
        with tqdm(total=100, desc="Loading CSV") as pbar:
            df = pd.read_csv(csv_file)
            pbar.update(100)

        # Use write_pandas to upload the DataFrame directly into the Snowflake table
        print("Uploading transactions to Snowflake...")
        success, nchunks, nrows, _ = write_pandas(conn, df, 'TRANSACTIONS')

        print(f"Successfully loaded {nrows} rows to the TRANSACTIONS table in Snowflake.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    upload_to_snowflake()
