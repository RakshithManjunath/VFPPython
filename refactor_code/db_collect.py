import csv
from dbfread import DBF
from psycopg2 import Error,connect
from datetime import timedelta  # Import timedelta
from test import file_paths

def collect_pg_data():
    table_paths = file_paths()
    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    print(start_date, end_date, type(start_date), type(end_date))  # 2024-02-01 2024-02-29 <class 'datetime.date'> <class 'datetime.date'>

    try:
        # Connect to the PostgreSQL database
        connection = connect(
            user="postgres",
            password="syscom",
            host="localhost",
            port="5432",
            database="postgres"
        )

        # Create a cursor object using cursor() method
        cursor = connection.cursor()

        # Adjust date range
        end_date_plus_1 = end_date + timedelta(days=1)

        # Execute a SELECT query
        cursor.execute("""
            SELECT *
            FROM wdtest
            WHERE punchdate::date >= %s AND punchdate::date < %s
            ORDER BY punchdate
        """, (start_date, end_date_plus_1))

        # Fetch all rows
        rows = cursor.fetchall()

        # Write fetched data to a CSV file
        with open(table_paths['wdtest_path'], 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow([desc[0] for desc in cursor.description])
            # Write rows
            csv_writer.writerows(rows)

    except (Exception, Error) as error:
        print("Error:", error)

    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
