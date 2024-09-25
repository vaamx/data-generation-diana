import snowflake.connector
import pandas as pd

# Snowflake connection details
def get_snowflake_connection():
    return snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='STOREFRONTS'
    )

# Fetch data from Snowflake using a SQL query
def fetch_data_from_snowflake(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Get column names
        df = pd.DataFrame(data, columns=columns)  # Convert data into a DataFrame
    return df

# Main function to fetch data and store it in a CSV file
def main():
    # SQL query to fetch the data you want
    query = "SELECT * FROM STORES LIMIT 1000;"  # Adjust your query as needed

    # Get Snowflake connection
    conn = get_snowflake_connection()
    
    try:
        # Fetch data
        df = fetch_data_from_snowflake(query, conn)
        
        # Save the data to a CSV file
        csv_file_path = 'snowflake_data.csv'
        df.to_csv(csv_file_path, index=False)
        
        print(f"Data has been successfully written to {csv_file_path}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    main()
