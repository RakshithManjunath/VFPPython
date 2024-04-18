import pandas as pd
from dbfread import DBF
from psycopg2 import connect, Error
from test import file_paths

def collect_pg_data():

    table_paths = file_paths()
    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    print(start_date, end_date, type(start_date), type(end_date))
    try:

        # Attempt to read the last row of the CSV to find the last `punch_date_time`
        try:
            existing_data = pd.read_csv('wdtest_temp.csv')
            # Assuming `punch_date_time` is in a sortable string format like 'YYYYMMDD HHMMSS'
            if not existing_data.empty:
                # If using both punch_date_time and empcode for uniqueness
                last_punch_date_time = existing_data.iloc[-1]['punch_date_time']
                last_empcode = existing_data.iloc[-1]['empcode']
            else:
                last_punch_date_time = '00000000 000000'  # Or some early equivalent
                last_empcode = None
        except FileNotFoundError:
            last_punch_date_time = '00000000 000000'  # Or some early equivalent
            last_empcode = None

        # Connect to the PostgreSQL database
        connection = connect(
            user="postgres",
            password="syscom",
            host="localhost",
            port="5432",
            database="postgres"
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Adjust the SQL query
        if last_empcode:
            query = """
                SELECT *
                FROM wdtest
                WHERE 
                    punchdate::date >= %s AND punchdate::date <= %s
                    AND (punch_date_time > %s OR (punch_date_time = %s AND empcode > %s))
                ORDER BY punch_date_time ASC, empcode ASC;
            """
            params = (start_date, end_date, last_punch_date_time, last_punch_date_time, last_empcode)
        else:
            query = """
                SELECT *
                FROM wdtest
                WHERE punchdate::date >= %s AND punchdate::date <= %s
                    AND punch_date_time > %s
                ORDER BY punch_date_time ASC;
            """
            params = (start_date, end_date, last_punch_date_time)

        cursor.execute(query, params)

        # Fetch all rows
        rows = cursor.fetchall()

        # if rows:
        column_names = [desc[0] for desc in cursor.description]
        new_records = pd.DataFrame(rows, columns=column_names)
        # Append to CSV
        new_records.to_csv(table_paths['wdtest_path'], index=False)

    except (Exception, Error) as error:
        print("Error:", error)
    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")