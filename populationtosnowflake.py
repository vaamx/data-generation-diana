import pandas as pd
import numpy as np
import snowflake.connector

# Load the CSV data
file_path = '/home/vaamx/Opscale/datasets/datasets/el_salvador_population_data.csv'
df = pd.read_csv(file_path)

# Add columns for AREA_SQ_KM and POPULATION_DENSITY
np.random.seed(42)  # Ensure reproducibility
df['AREA_SQ_KM'] = np.random.uniform(10, 100, len(df))
df['POPULATION_DENSITY'] = df['TOTAL'] / df['AREA_SQ_KM']

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='OPSCALEAI',
    password='Opscale2030',
    account='nvvmnod-mw08757',
    warehouse='DIANA_DATA_LAKE',
    database='DIANA_SALES_ES',
    schema='STOREFRONTS'
)

cursor = conn.cursor()

# Prepare insert statement
insert_query = """
INSERT INTO POPULATIONDENSITY (MUNICIPALITY, TOTAL_POPULATION, AREA_SQ_KM, POPULATION_DENSITY)
VALUES (%s, %s, %s, %s)
"""

# Insert the data row by row
for _, row in df.iterrows():
    cursor.execute(insert_query, (row['MUNICIPIO'], row['TOTAL'], row['AREA_SQ_KM'], row['POPULATION_DENSITY']))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
