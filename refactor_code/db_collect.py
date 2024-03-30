import csv
from dbfread import DBF
from psycopg2 import Error,connect
import pandas as pd
# from datetime import timedelta  # Import timedelta
from test import file_paths

def standardize_datetime(datetime_str):
    # If '-' in string, it's in 'YYYY-MM-DD HH:MM:SS' format
    if '-' in datetime_str:
        datetime_obj = pd.to_datetime(datetime_str, format='%Y-%m-%d %H:%M:%S')
    else:
        # Otherwise, it's in 'YYYYMMDD HHMMSS' format
        datetime_obj = pd.to_datetime(datetime_str, format='%Y%m%d %H%M%S')
    
    # Return standardized format 'YYYYMMDD HHMMSS'
    return datetime_obj.strftime('%Y%m%d %H%M%S')

def collect_pg_data():
    table_paths = file_paths()
    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    print(start_date, end_date, type(start_date), type(end_date))  # 2024-02-01 2024-02-29 <class 'datetime.date'> <class 'datetime.date'>

    punches_table = DBF(table_paths['punches_dbf_path'], load=True)
    punches_from_dbf = pd.DataFrame(iter(punches_table))
    if len(punches_from_dbf) > 0:
        punches_from_dbf = punches_from_dbf[(punches_from_dbf['PDATE'] >= start_date) & (punches_from_dbf['PDATE'] <= end_date)]
        punches_from_dbf['PDTIME'] = pd.to_datetime(punches_from_dbf['PDTIME'])
        punches_from_dbf['PDTIME'] = punches_from_dbf['PDTIME'].dt.strftime('%Y-%m-%d %H:%M')
        punches_from_dbf.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        punches_from_dbf['PDTIME'] = punches_from_dbf['PDTIME'].apply(standardize_datetime)
        # punches_from_dbf['PDTIME'] = punches_from_dbf['PDTIME'] + ' ' + punches_from_dbf['TOKEN'].astype(str) + ' ' + punches_from_dbf['MCIP'].astype(str)
        # punches_from_dbf.to_csv('db_collect.csv',index=False)

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
        # end_date_plus_1 = end_date + timedelta(days=1)

        # Execute a SELECT query
        cursor.execute("""
            SELECT *
            FROM wdtest
            WHERE punchdate::date >= %s AND punchdate::date <= %s
            ORDER BY punch_date_time ASC
        """, (start_date, end_date))

        # Fetch all rows
        rows = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]
        punches_from_postgres = pd.DataFrame(rows, columns=column_names)
        punches_from_postgres['punch_date_time'] = punches_from_postgres['punch_date_time'].apply(standardize_datetime)
        punches_from_postgres['punch_date_time'] = punches_from_postgres['punch_date_time'] + ' ' + punches_from_postgres['empcode'].astype(str) + ' ' + punches_from_postgres['terminal_sl'].astype(str)
        print(punches_from_postgres)

        # Write DataFrame to a CSV file using pandas
        punches_from_postgres.to_csv(table_paths['wdtest_path'], index=False)

    except (Exception, Error) as error:
        print("Error:", error)

    finally:
        # Closing database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
