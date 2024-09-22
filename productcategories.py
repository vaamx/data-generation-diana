import pandas as pd

# Path to the CSV file
file_path = '/home/vaamx/Opscale/datasets/datasets/diana_products_dynamic.csv'

# Read the CSV file
data = pd.read_csv(file_path)

# Assuming the column containing the product categories is named 'category'
# If the column name is different, replace 'category' with the correct column name.
if 'category' in data.columns:
    # Extract unique categories
    unique_categories = data['category'].dropna().unique()
else:
    print("Category column not found in the dataset. Please check the column names.")
    unique_categories = []

# Create contextual descriptions for each category
category_descriptions = {
    "Snacks Salados": "A wide range of salty snacks, perfect for quick bites, including nachos and chips.",
    "Dulces": "Delicious sweet treats, ranging from candies to chocolates, enjoyed by all ages.",
    "Bebidas": "Refreshing beverages, from sodas to fruit juices, available in various flavors.",
    "Productos Lácteos": "Fresh dairy products including milk, cheese, and yogurt, sourced locally.",
    "Carnes": "Premium quality meats, including beef, chicken, and pork, perfect for any meal.",
    # Add more categories with their contextual descriptions
}

# Generate data for ProductCategories with incremental Category IDs starting at 1000
category_data = {
    "CATEGORY_ID": [1000 + i * 1000 for i in range(len(unique_categories))],
    "CATEGORY_NAME": unique_categories,
    "DESCRIPTION": [category_descriptions.get(cat, "General category description.") for cat in unique_categories]
}

# Create DataFrame
category_df = pd.DataFrame(category_data)

# Save the DataFrame to CSV
output_path = '/home/vaamx/Opscale/datasets/datasets/product_categories.csv'
category_df.to_csv(output_path, index=False)

print(f"Product categories with descriptions generated and saved successfully to {output_path}!")
