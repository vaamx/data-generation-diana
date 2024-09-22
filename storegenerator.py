import pandas as pd
import snowflake.connector
import random

# Function to generate store names based on specified lists and probabilities
def generate_store_name():
    popular_store_names = [
        "Wal-Mart", "Super Selectos", "Despensa de Don Juan", "Dollar City", 
        "La Despensa de Don Juan", "Maxi Despensa", "Supermercado Europa"
    ]
    if random.random() < 0.2:  # 20% chance to be a popular store
        return random.choice(popular_store_names)
    else:
        prefixes = [
            "Tienda", "Super", "Comercial", "Almacen", "Mercadito", "Punto de Venta",
            "Mini Market", "El Rinconcito", "La Esquina", "La Bodega", "El Mercadillo",
            "Abarrotes", "La Tiendona", "La Despensa", "La Central", "Distribuidora",
            "Mayorista", "Tiendita", "El Almacen", "Supermercado", "Mercadito Familiar",
            "Plaza Market", "Mercado Local", "Tienda Familiar", "Tienda de la Esquina",
            "Maxi Ahorro", "El Ahorrador", "Mercado Popular", "Mercado de la Fe",
            "El Buen Precio", "Bodega de Descuentos", "Mercado El Centro", "Super Precio",
            "Super Oferta"
        ]
        names = [
            "La Esperanza", "San Jose", "El Sol", "Buen Precio", "El Ahorro", "Nuevo Mundo",
            "Los Primos", "El Centro", "Santa Maria", "La Fe", "San Francisco", "El Milagro",
            "Santa Clara", "La Unión", "Los Ángeles", "El Buen Gusto", "La Fortuna",
            "El Progreso", "El Rosario", "San Carlos"
        ]
        suffixes = ["Central", "Sur", "Norte", "Este", "Oeste", "Del Pueblo", "Express"]
        return f"{random.choice(prefixes)} {random.choice(names)} {random.choice(suffixes)}"

# Generate a more general store description
def generate_store_description():
    sizes = ["pequeña", "mediana", "grande"]
    access = ["fácil acceso", "acceso restringido", "acceso complicado"]
    locations = ["en el centro", "en la periferia", "en zona residencial", "en zona comercial"]
    notes = [
        "con amplio estacionamiento", "sin estacionamiento", "con áreas verdes cercanas",
        "cerca de otras tiendas", "aislada de otros comercios", "con mucha afluencia de gente",
        "con poca afluencia de gente"
    ]
    
    description = f"Tienda de tamaño {random.choice(sizes)}, {random.choice(access)}, ubicada {random.choice(locations)}, {random.choice(notes)}."
    return description

# Function to connect to Snowflake and fetch region data
def fetch_region_data():
    conn = snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='STOREFRONTS'
    )
    query = "SELECT REGION_ID, DEPARTMENT, MUNICIPALITY FROM REGION;"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Main function to generate stores and save locally
def generate_stores():
    region_data = fetch_region_data()
    stores = []
    for _, row in region_data.iterrows():
        for _ in range(2):  # Generate 2 stores per Department/Municipality
            store = {
                'STORE_NAME': generate_store_name(),
                'STORE_DESCRIPTION': generate_store_description(),
                'ADDRESS': f"Residencial {row['MUNICIPALITY']}",
                'CONTACT_NAME': 'John Doe',  # Placeholder for example
                'CONTACT_EMAIL': 'email@example.com',  # Placeholder for example
                'CONTACT_PHONE': '+503 7000 0000',  # Placeholder for example
                'REGION_ID': row['REGION_ID']
            }
            stores.append(store)
    # Convert list of dictionaries to DataFrame
    df_stores = pd.DataFrame(stores)
    # Save DataFrame to CSV
    df_stores.to_csv('/home/vaamx/Opscale/Demo/Diana/generated_stores.csv', index=False)

if __name__ == "__main__":
    generate_stores()

