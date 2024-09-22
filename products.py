import pandas as pd
import numpy as np
from faker import Faker
import snowflake.connector
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

def fetch_data():
    try:
        # Connection parameters
        conn_params = {
            "user": 'OPSCALEAI',
            "password": 'Opscale2030',
            "account": 'nvvmnod-mw08757',
            "warehouse": 'DIANA_DATA_LAKE',
            "database": 'DIANA_SALES_ES',
            "schema": 'SALES'
        }

        # SQL query
        category_query = "SELECT * FROM SALES.PRODUCTCATEGORIES"

        # Using a context manager to ensure the connection is closed after the block
        with snowflake.connector.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                # Execute a query
                cursor.execute("USE WAREHOUSE DIANA_DATA_LAKE")
                # Fetching the data into a DataFrame
                df = pd.read_sql(category_query, conn)
                print(df)

                # Further operations
                category_query = "SELECT CATEGORY_ID, CATEGORY_NAME FROM PRODUCTCATEGORIES;"
                category_df = pd.read_sql(category_query, conn)

                # Load existing product data
                file_path = '/home/vaamx/Opscale/datasets/datasets/diana_products_dynamic.csv'
                data = pd.read_csv(file_path)

                # Map categories to category IDs using fetched data
                category_map = dict(zip(category_df['CATEGORY_NAME'], category_df['CATEGORY_ID']))
                data['PRODUCT_CATEGORY_ID'] = data['category'].map(category_map)

                # Generate data
                data['PRICE'] = np.round(np.random.uniform(0.50, 2.50, size=len(data)), 2)
                today = datetime.today()
                data['PRODUCTION_DATE'] = [fake.date_between(start_date=today - timedelta(days=7), end_date='today') for _ in range(len(data))]

                # Enhanced description generator
                def generate_description(name, category):
                    base_description = {
                        'Snacks Salados': "Crisp and savory, ideal for a quick snack.",
                        'Dulces': "Sweet and delightful, perfect for a treat.",
                        # Add more base descriptions for other categories
                    }
                    return f"{name} offers a unique taste experience, {base_description.get(category, 'enjoyable and satisfying')}."

                data['DESCRIPTION'] = data.apply(lambda x: generate_description(x['name'], x['category']), axis=1)

                # Prepare data for Snowflake insertion
                data.rename(columns={'name': 'PRODUCT_NAME'}, inplace=True)
                data.drop(columns=['category', 'subcategory', 'introduction_date'], inplace=True)

                # Save the processed data
                output_path =output_path = '/home/vaamx/Opscale/Demo/Diana/processed_products.csv'
                data.to_csv(output_path, index=False)

                print(f"Processed data saved to {output_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation complete.")

if __name__ == "__main__":
    fetch_data()
