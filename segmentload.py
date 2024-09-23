import snowflake.connector
from snowflake.connector.errors import ProgrammingError

# Define your segment data
segments = [
    {"id": 1, "name": "Diamante", "description": "Clientes más valiosos y estratégicos."},
    {"id": 2, "name": "Oro", "description": "Clientes importantes con potencial de crecimiento."},
    {"id": 3, "name": "Plata", "description": "Clientes regulares con compras moderadas."},
    {"id": 4, "name": "Cobre", "description": "Clientes con menor volumen de compras pero con potencial de desarrollo."},
    {"id": 5, "name": "Hierro", "description": "Clientes de bajo compromiso actual pero que representan oportunidades de mercado."}
]

# Function to insert segment data
def load_segments(connection, segment_data):
    cursor = connection.cursor()
    try:
        for segment in segment_data:
            query = """
            INSERT INTO DIANA_SALES_ES.SEGMENTS.SEGMENTS (SEGMENT_ID, SEGMENT_NAME, DESCRIPTION)
            VALUES (%s, %s, %s)  # Make sure there are three %s placeholders
            """
            # Ensure the tuple contains three elements corresponding to the placeholders
            cursor.execute(query, (segment['id'], segment['name'], segment['description']))
        print("Segments data loaded successfully.")
    except ProgrammingError as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        
# Main function to manage the database connection and call the load function
def main():
    try:
        # Set up connection parameters
        conn = snowflake.connector.connect(
            user='OPSCALEAI',
            password='Opscale2030',
            account='nvvmnod-mw08757',
            warehouse='DIANA_DATA_LAKE',
            database='DIANA_SALES_ES',
            schema='SEGMENTS'
        )
        load_segments(conn, segments)
    except Exception as e:
        print(f"Failed to connect or execute: {e}")
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
