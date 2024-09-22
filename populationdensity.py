import pandas as pd
import numpy as np

# Load the CSV data
file_path = '/home/vaamx/Opscale/datasets/datasets/el_salvador_population_data.csv'
df = pd.read_csv(file_path)

# Add columns for AREA_SQ_KM and POPULATION_DENSITY
# Simulate AREA_SQ_KM (let's assume values between 10 to 100 sq km for example purposes)
np.random.seed(42)  # Ensure reproducibility
df['AREA_SQ_KM'] = np.random.uniform(10, 100, len(df))

# Calculate POPULATION_DENSITY = TOTAL_POPULATION / AREA_SQ_KM
df['POPULATION_DENSITY'] = df['TOTAL'] / df['AREA_SQ_KM']

# Display the updated DataFrame
df.head()

# Save the DataFrame to a new CSV file
df.to_csv('/home/vaamx/Opscale/Demo/Diana/data_generation/populationdensity.csv', index=False)
