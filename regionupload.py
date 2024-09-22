import snowflake.connector
import pandas as pd

# Function to load data into the REGION table
def load_data_to_region(conn, file_path):
    try:
        # Load data from CSV into a DataFrame
        # Specifying delimiter and quote character as per your CSV's format
        data = pd.read_csv(file_path, delimiter=',', quotechar='"')
        
        # Prepare insert statement
        insert_query = """
        INSERT INTO DIANA_SALES_ES.STOREFRONTS.REGION (DEPARTMENT, MUNICIPALITY, TOTAL_POPULATION, URBAN_POPULATION, RURAL_POPULATION)
        VALUES (%s, %s, %s, %s, %s);
        """
        
        # Execute insert for each row in the DataFrame
        with conn.cursor() as cursor:
            for index, row in data.iterrows():
                cursor.execute(insert_query, (row['DEPARTAMENTO'], row['MUNICIPIO'], row['TOTAL'], row['URBANO'], row['RURAL']))
        
        print("Data loaded successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Main function to initiate the connection and call the loading function
def main():
    conn = snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='STOREFRONTS'
    )
    
    file_path = '/home/vaamx/Opscale/datasets/datasets/el_salvador_population_data.csv'  # Correct path to the CSV file
    
    try:
        load_data_to_region(conn, file_path)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
