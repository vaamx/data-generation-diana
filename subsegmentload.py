import snowflake.connector

# Connection parameters
conn_params = {
    "user": 'OPSCALEAI',
    "password": 'Opscale2030',
    "account": 'nvvmnod-mw08757',
    "warehouse": 'DIANA_DATA_LAKE',
    "database": 'DIANA_SALES_ES',
    "schema": 'SEGMENTS'
}

# Subsegment data
# Assuming segment IDs are already known from earlier loading; else fetch them first.
subsegments = [
    {"segment_id": 1, "name": "Esmeralda", "description": "Highest value clients with exceptional loyalty."},
    {"segment_id": 1, "name": "Rubí", "description": "High spending frequent clients."},
    {"segment_id": 1, "name": "Zafiro", "description": "Loyal and high-value clients."},
    {"segment_id": 101, "name": "Topacio", "description": "Growing clients with significant potential."},
    {"segment_id": 101, "name": "Citrino", "description": "Solid performing clients with good potential."},
    {"segment_id": 101, "name": "Ámbar", "description": "Regular clients on the verge of upgrading."},
    {"segment_id": 102, "name": "Perla", "description": "Stable regular clients with moderate engagement."},
    {"segment_id": 102, "name": "Ópalo", "description": "Occasional clients with specific product loyalty."},
    {"segment_id": 102, "name": "Turquesa", "description": "Low engagement clients with potential for growth."},
    {"segment_id": 2, "name": "Malaquita", "description": "Clients with potential for development."},
    {"segment_id": 2, "name": "Azurita", "description": "Clients influenced by specific campaigns."},
    {"segment_id": 2, "name": "Jade", "description": "Inactive clients who can be reactivated with targeted strategies."},
    {"segment_id": 3, "name": "Hematita", "description": "Price-sensitive clients influenced by competition."},
    {"segment_id": 3, "name": "Pirita", "description": "Minimal-purchase clients, requiring stimulation for higher engagement."},
    {"segment_id": 3, "name": "Magnetita", "description": "Potential clients yet to be explored."},
]

# SQL command template
sql_insert = """
INSERT INTO SUBSEGMENTS (SEGMENT_ID, SUBSEGMENT_NAME, DESCRIPTION)
VALUES (%s, %s, %s);
"""

# Establish connection and insert data
try:
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()
    for subsegment in subsegments:
        cur.execute(sql_insert, (subsegment["segment_id"], subsegment["name"], subsegment["description"]))
    print("Subsegments inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
