import snowflake.connector
from snowflake.connector import DictCursor

# Function to fetch subsegment IDs from the database
def fetch_subsegment_ids(conn):
    query = "SELECT SUBSEGMENT_ID, SUBSEGMENT_NAME FROM SUBSEGMENTS;"
    with conn.cursor(DictCursor) as cursor:
        cursor.execute(query)
        results = {row['SUBSEGMENT_NAME']: row['SUBSEGMENT_ID'] for row in cursor.fetchall()}
    return results

# Function to insert segment criteria into the database
def insert_segment_criteria(conn, subsegment_ids):
    # Define the criteria for each subsegment
    criteria = [
        {"subsegment_id": subsegment_ids["Esmeralda"], "vac_min": 150001, "vac_max": None, "fc_min": 13, "fc_max": None, "ac_min": 8, "ac_max": None, "vmc_min": 12501, "vmc_max": None, "ruc_max": 15, "il_description": "Excelente"},
        {"subsegment_id": subsegment_ids["Rubí"], "vac_min": 120001, "vac_max": 150000, "fc_min": 10, "fc_max": 12, "ac_min": 5, "ac_max": 7, "vmc_min": 10000, "vmc_max": 12500, "ruc_max": 30, "il_description": "Muy Alto"},
        {"subsegment_id": subsegment_ids["Zafiro"], "vac_min": 100001, "vac_max": 120000, "fc_min": 8, "fc_max": 10, "ac_min": 5, "ac_max": 7, "vmc_min": 8500, "vmc_max": 10000, "ruc_max": 45, "il_description": "Alto"},
        {"subsegment_id": subsegment_ids["Topacio"], "vac_min": 80001, "vac_max": 100000, "fc_min": 7, "fc_max": 8, "ac_min": 4, "ac_max": 5, "vmc_min": 10000, "vmc_max": 12500, "ruc_max": 45, "il_description": "Muy Alto"},
        {"subsegment_id": subsegment_ids["Citrino"], "vac_min": 65001, "vac_max": 80000, "fc_min": 6, "fc_max": 7, "ac_min": 3, "ac_max": 4, "vmc_min": 8000, "vmc_max": 10000, "ruc_max": 60, "il_description": "Alto"},
        {"subsegment_id": subsegment_ids["Ámbar"], "vac_min": 50001, "vac_max": 65000, "fc_min": 5, "fc_max": 6, "ac_min": 3, "ac_max": 4, "vmc_min": 6250, "vmc_max": 8000, "ruc_max": 75, "il_description": "Alto"},
        {"subsegment_id": subsegment_ids["Perla"], "vac_min": 40001, "vac_max": 50000, "fc_min": 4, "fc_max": 5, "ac_min": 2, "ac_max": 3, "vmc_min": 5000, "vmc_max": 6250, "ruc_max": 90, "il_description": "Medio-Alto"},
        {"subsegment_id": subsegment_ids["Ópalo"], "vac_min": 25001, "vac_max": 40000, "fc_min": 3, "fc_max": 4, "ac_min": 1.5, "ac_max": 2, "vmc_min": 3000, "vmc_max": 5000, "ruc_max": 120, "il_description": "Medio"},
        {"subsegment_id": subsegment_ids["Turquesa"], "vac_min": 10001, "vac_max": 25000, "fc_min": 2, "fc_max": 3, "ac_min": 1, "ac_max": 1.5, "vmc_min": 1250, "vmc_max": 3000, "ruc_max": 150, "il_description": "Medio-Bajo"},
        {"subsegment_id": subsegment_ids["Malaquita"], "vac_min": 7501, "vac_max": 10000, "fc_min": 2, "fc_max": 2, "ac_min": 0.75, "ac_max": 1, "vmc_min": 1000, "vmc_max": 1250, "ruc_max": 150, "il_description": "Bajo"},
        {"subsegment_id": subsegment_ids["Azurita"], "vac_min": 4001, "vac_max": 7500, "fc_min": 1, "fc_max": 2, "ac_min": 0.5, "ac_max": 0.75, "vmc_min": 500, "vmc_max": 1000, "ruc_max": 180, "il_description": "Bajo"},
        {"subsegment_id": subsegment_ids["Jade"], "vac_min": 1001, "vac_max": 4000, "fc_min": 1, "fc_max": 1, "ac_min": 0.5, "ac_max": 0.75, "vmc_min": 125, "vmc_max": 500, "ruc_max": 210, "il_description": "Muy Bajo"},
        {"subsegment_id": subsegment_ids["Hematita"], "vac_min": 751, "vac_max": 1000, "fc_min": 0.5, "fc_max": 1, "ac_min": 0.25, "ac_max": 0.5, "vmc_min": 100, "vmc_max": 125, "ruc_max": 240, "il_description": "Muy Bajo"},
        {"subsegment_id": subsegment_ids["Pirita"], "vac_min": 251, "vac_max": 750, "fc_min": 0.25, "fc_max": 0.5, "ac_min": 0.1, "ac_max": 0.25, "vmc_min": 50, "vmc_max": 100, "ruc_max": 300, "il_description": "Muy Bajo"},
        {"subsegment_id": subsegment_ids["Magnetita"], "vac_min": 0, "vac_max": 250, "fc_min": 0, "fc_max": 0.25, "ac_min": 0, "ac_max": 0.1, "vmc_min": 0, "vmc_max": 50, "ruc_max": 300, "il_description": "Nulo"}
    ]

    # SQL template for inserting data
    sql_insert = """
    INSERT INTO SEGMENTCRITERIA (SUBSEGMENT_ID, VAC_MIN, VAC_MAX, FC_MIN, FC_MAX, AC_MIN, AC_MAX, VMC_MIN, VMC_MAX, RUC_MAX, IL_DESCRIPTION)
    VALUES (%(subsegment_id)s, %(vac_min)s, %(vac_max)s, %(fc_min)s, %(fc_max)s, %(ac_min)s, %(ac_max)s, %(vmc_min)s, %(vmc_max)s, %(ruc_max)s, %(il_description)s);
    """
    
    # Execute insert statements
    with conn.cursor() as cursor:
        for item in criteria:
            cursor.execute(sql_insert, item)

# Main function to establish connection and initiate data insertion
def main():
    conn = snowflake.connector.connect(
        user='OPSCALEAI',
        password='Opscale2030',
        account='nvvmnod-mw08757',
        warehouse='DIANA_DATA_LAKE',
        database='DIANA_SALES_ES',
        schema='SEGMENTS'
    )
    
    try:
        subsegment_ids = fetch_subsegment_ids(conn)
        insert_segment_criteria(conn, subsegment_ids)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
