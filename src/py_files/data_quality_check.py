from datetime import datetime, timedelta
import psycopg2
import pandas as pd

def create_diff_report(response1, response2):
    symbols1 = set(response1['symbol'].dropna())
    symbols2 = set(response2['symbol'].dropna())

    new_symbols = symbols2 - symbols1
    missing_symbols = symbols1 - symbols2

    common_symbols = symbols1 & symbols2

    response1_common = response1[response1['symbol'].isin(common_symbols)].set_index('symbol')
    response2_common = response2[response2['symbol'].isin(common_symbols)].set_index('symbol')

    response1_common = response1_common.sort_index()
    response2_common = response2_common.sort_index()

    numeric_fields = response1_common.select_dtypes(include=['number']).columns
    numeric_fields = numeric_fields.drop('ResponseDatetime', errors='ignore')

    correlation_report = response1_common[numeric_fields].corrwith(response2_common[numeric_fields])
    
    correlation_report = correlation_report.reset_index()
    correlation_report.columns = ['field', 'correlation']

    result = {
        "new_symbols": list(new_symbols),
        "missing_symbols": list(missing_symbols),
        "correlation_report": correlation_report
    }

    return result


# Database connection
def get_db_connection():
    return psycopg2.connect(
        host="dpg-cgfqdvseoogqfc6p8mlg-a.ohio-postgres.render.com",
        port=5432,
        database="financial_data",
        user="Nachiket",
        password="Nachiket'sAccess"
    )


def fetch_report_run_id():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COALESCE(MAX(report_run_id), 0) + 1 FROM data_comparison_results;")
    report_run_id = cursor.fetchone()[0]
    cursor.close()
    return report_run_id
    

def fetch_datetimes(**kwargs):
    conn = get_db_connection()
    mode = kwargs['mode']

    cursor = conn.cursor()

    if mode == "consecutive":
        today = datetime.utcnow().date() - timedelta(days=11)
        print(f"today is {today}")
        cursor.execute("""
        SELECT DISTINCT "ResponseDatetime"
        FROM parsed_stock_data
        WHERE DATE("ResponseDatetime") = %s
        ORDER BY "ResponseDatetime" DESC;
        """, (today,))
        datetimes = [row[0] for row in cursor.fetchall()]
       
        print(f"datetimes {datetimes}")
        if len(datetimes) < 2:
            raise ValueError("Not enough responses to perform a consecutive comparison.")
        
        kwargs['ti'].xcom_push(key='ordered_datetimes', value=datetimes)
        
        # kwargs['ti'].xcom_push(key='datetime1', value=datetimes[1][0])
        # kwargs['ti'].xcom_push(key='datetime2', value=datetimes[0][0])

    elif mode == "day_comparison":
        today = datetime.utcnow().date() - timedelta(days=11)
        yesterday = today - timedelta(days=1)

        cursor.execute("""
        SELECT MIN("ResponseDatetime") AS earliest_today
        FROM parsed_stock_data
        WHERE DATE("ResponseDatetime") = %s;
        """, (today,))
        earliest_today = cursor.fetchone()[0]

        cursor.execute("""
        SELECT MAX("ResponseDatetime") AS latest_yesterday
        FROM parsed_stock_data
        WHERE DATE("ResponseDatetime") = %s;
        """, (yesterday,))
        latest_yesterday = cursor.fetchone()[0]

        if not earliest_today or not latest_yesterday:
            raise ValueError("Not enough responses for day comparison.")
        kwargs['ti'].xcom_push(key='datetime1', value=latest_yesterday)
        kwargs['ti'].xcom_push(key='datetime2', value=earliest_today)
    
    elif mode == "same_interval":
        today = datetime.utcnow().date() - timedelta(days=11)
        print(f"today si {today}")
        yesterday = today - timedelta(days=1)

        cursor.execute("""
        SELECT MIN("ResponseDatetime") AS earliest_today
        FROM parsed_stock_data
        WHERE DATE("ResponseDatetime") = %s;
        """, (today,))
        earliest_today = cursor.fetchone()[0]

        cursor.execute("""
        SELECT MIN("ResponseDatetime") as earliest_yesterday
        FROM parsed_stock_data
        WHERE DATE("ResponseDatetime") = %s;
        """, (yesterday,))
        earliest_yesterday = cursor.fetchone()[0]

        if not earliest_today or not earliest_yesterday:
            raise ValueError("Not enough response for comparison")
        
        kwargs['ti'].xcom_push(key='datetime1', value=earliest_yesterday)
        kwargs['ti'].xcom_push(key='datetime2', value=earliest_today)


    conn.close()


def compare_all_consecutive_responses(**kwargs):
    
    ordered_datetimes = kwargs['ti'].xcom_pull(key='ordered_datetimes')
    conn = get_db_connection()

    results = []
    for i in range(len(ordered_datetimes) - 1):
        datetime1 = ordered_datetimes[i + 1]  # Older
        datetime2 = ordered_datetimes[i]      # Newer

        query = """
        SELECT * FROM parsed_stock_data WHERE "ResponseDatetime" = %s;
        """
        response1 = pd.read_sql_query(query, conn, params=(datetime1,))
        response2 = pd.read_sql_query(query, conn, params=(datetime2,))

        result = create_diff_report(response1, response2)
        results.append({
            'datetime1': datetime1,
            'datetime2': datetime2,
            'new_symbols': result['new_symbols'],
            'missing_symbols': result['missing_symbols'],
            'correlation_report': result['correlation_report']
        })

    conn.close()

    # Output or store the results
    for res in results:
        print(f"Comparison between {res['datetime1']} and {res['datetime2']}:")
        print(f"New Symbols: {res['new_symbols']}")
        print(f"Missing Symbols: {res['missing_symbols']}")
        print("Field Correlation Report:")
        print(res['correlation_report'])


def populate_data_comparison_results(result, dataset_a_datetime, dataset_b_datetime, datasource_a, datasource_b, report_run_id, diff_report_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    for _, row in result['correlation_report'].iterrows():

        print("Arguments:", (
            report_run_id,
            diff_report_id,
            datasource_a,
            datasource_b,
            row['field'],
            "Correlation",
            row['correlation'],
            dataset_a_datetime,
            dataset_b_datetime
        ))
        insert_query = """
        INSERT INTO data_comparison_results (
            "report_run_id", "comparison_report_version_id", "datasource_a", "datasource_b", "comparison_data_field", "comparison_measure",
            "comparison_value", "dataset_a_datetime", "dataset_b_datetime"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute( insert_query, (
            report_run_id,
            diff_report_id,
            datasource_a,
            datasource_b,
            row['field'],
            "Correlation",
            row['correlation'],
            dataset_a_datetime,
            dataset_b_datetime
        ))

    insert_query = """
    INSERT INTO data_comparison_results (
        "report_run_id", "comparison_report_version_id", "datasource_a", "datasource_b", "comparison_data_field", "comparison_measure",
        "comparison_value", "dataset_a_datetime", "dataset_b_datetime"
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (
        report_run_id,
        diff_report_id,
        datasource_a,
        datasource_b,
        "Symbol",
        "SetA-SetB",
        result['missing_symbols'] if result['missing_symbols'] else None,
        dataset_a_datetime,
        dataset_b_datetime
    ))
    
    insert_query = """
    INSERT INTO data_comparison_results (
        "report_run_id", "comparison_report_version_id", "datasource_a", "datasource_b", "comparison_data_field", "comparison_measure",
        "comparison_value", "dataset_a_datetime", "dataset_b_datetime"
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (
        report_run_id,
        diff_report_id,
        datasource_a,
        datasource_b,
        "Symbol",
        "SetB-SetA",
        result['new_symbols'] if result['new_symbols'] else None,
        dataset_a_datetime,
        dataset_b_datetime 
    ))

    conn.commit()
    cursor.close()
    conn.close()

def extract_data(**kwargs):
    conn = get_db_connection()
    datetime1 = kwargs['ti'].xcom_pull(key='datetime1')
    datetime2 = kwargs['ti'].xcom_pull(key='datetime2')

    query = """
    SELECT * FROM parsed_stock_data WHERE "ResponseDatetime" = %s;
    """
    response1 = pd.read_sql_query(query, conn, params=(datetime1,))
    response2 = pd.read_sql_query(query, conn, params=(datetime2,))

    conn.close()
    kwargs['ti'].xcom_push(key='response1', value=response1.to_json())
    kwargs['ti'].xcom_push(key='response2', value=response2.to_json())

def compare_responses(**kwargs): 
    response1 = pd.read_json(kwargs['ti'].xcom_pull(key='response1'))
    response2 = pd.read_json(kwargs['ti'].xcom_pull(key='response2'))

    datetime1 = kwargs['ti'].xcom_pull(key='datetime1')
    datetime2 = kwargs['ti'].xcom_pull(key='datetime2')

    result = create_diff_report(response1, response2)

    # Example: Log the results (you can save them to a database or file)
    print(f"Comparison between {datetime1} and {datetime2}")
    print("New Symbols:", result['new_symbols'])
    print("Missing Symbols:", result['missing_symbols'])
    print("Field Correlation Report:")
    print(result['correlation_report'])

    report_run_id = fetch_report_run_id()
    print(f"report run id is {report_run_id}")
    
    populate_data_comparison_results(result, datetime1, datetime2, "FMP", "FMP", report_run_id, "Version_1" )