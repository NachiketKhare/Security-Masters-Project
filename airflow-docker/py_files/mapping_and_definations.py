from datetime import datetime, timedelta, timezone
import json
import psycopg2

# Mapping Table Functions 

def get_db_connection():
    return psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",
        user="Nachiket",
        password="Nachiket'sAccess"
    )


def insert_or_update_mappings(**kwargs):

    conn = get_db_connection()
    cursor = conn.cursor()

    field_mappings = [
        (["FMP"], "bidPrice", "current_nbbo_bid_price", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "bidSize", "current_nbbo_bid_size", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "askPrice", "current_nbbo_ask_price", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "askSize", "current_nbbo_ask_size", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "fmpLast", "last_trade_price", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "lastSaleSize", "last_trade_volume", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["FMP"], "lastSalePrice", "last_trade_price", "https://financialmodelingprep.com/api/v3/stock/full/real-time-price"),
        (["Yahoo Finance"], "bid", "current_nbbo_bid_price", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "bidSize", "current_nbbo_bid_size", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "ask", "current_nbbo_ask_price", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "askSize", "current_nbbo_ask_size", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "regularMarketPrice", "last_trade_price", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "regularMarketVolume", "last_trade_volume", "https://query1.finance.yahoo.com/v7/finance/quote"),
        (["Yahoo Finance"], "currentPrice", "last_trade_price", "https://query1.finance.yahoo.com/v7/finance/quote")
    ]

    today = datetime.now(timezone.utc) 

    for datasource, data_field, aDD_concept, api_endpoint in field_mappings:
        cursor.execute("""
        SELECT COUNT(*) FROM "datasource_field_to_aDD_concept_map"
        WHERE "DataSource" = CAST(%s AS character[]) AND "aDD_concept" = %s;
        """, (datasource, aDD_concept))

        exists = cursor.fetchone()[0] > 0

        if not exists:
            insert_query = """
            INSERT INTO "datasource_field_to_aDD_concept_map" 
                ("DataSource", "data_field", "aDD_concept", "DataSource_api_endpoint", "mapping_start_datetime", "mapping_end_datetime")
            VALUES (%s, %s, %s, %s, %s, NULL)
            """
            cursor.execute(insert_query, (datasource, data_field, aDD_concept, api_endpoint, today))
        
        else:
            update_query = """
            UPDATE "datasource_field_to_aDD_concept_map"
            SET mapping_end_datetime = %s
            WHERE "DataSource" = %s AND "data_field" = %s AND "aDD_concept" = %s;
            """
            cursor.execute(update_query, (datasource, data_field, aDD_concept))
            
    conn.commit()
    cursor.close()
    conn.close()

def insert_update_definitions(**kwargs):
    conn = get_db_connection()
    cursor = conn.cursor()

    json_file_path = "/opt/airflow/json_files/aDD_concept_definitions.json"
    with open(json_file_path, "r") as file:
        definitions = json.load(file)

    today = datetime.now(timezone.utc)

    for aDD_concept, concept_info in definitions.items():
        definition = concept_info["definition"]
        data_type = concept_info["data_type"]

        cursor.execute("""
        SELECT "definition", "data_type" 
        FROM "aDD_concept_definitions"
        WHERE "aDD_concept" = %s AND "definition_end_datetime" IS NULL;
        """, (aDD_concept,))

        exists = cursor.fetchone()

        if exists:
            current_definition, current_data_type = exists

            if current_definition != definition or current_data_type != data_type:
                update_query = """
                UPDATE "aDD_concept_definitions" 
                SET "definition_end_datetime" = %s
                WHERE "aDD_concept" = %s AND "definition_end_datetime" IS NULL;
                """
                cursor.execute(update_query, (today, aDD_concept))

                insert_query = """
                INSERT INTO "add_concept_definitions" (
                    "aDD_concept", "definition", "definition_start_datetime", "definition_end_datetime", "data_type"
                )
                VALUES (%s, %s, %s, NULL, %s);
                """
                cursor.execute(insert_query, (aDD_concept, definition, today, data_type))
        else:
            insert_query = """
            INSERT INTO "aDD_concept_definitions" (
                "aDD_concept", "definition", "definition_start_datetime", "definition_end_datetime", "data_type"
            )
            VALUES (%s, %s, %s, NULL, %s);
            """       
            cursor.execute(insert_query, (aDD_concept, definition, today, data_type))
        
    conn.commit()
    cursor.close()
    conn.close()
