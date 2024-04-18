from fastapi import FastAPI, HTTPException
import pandas as pd
from psycopg2 import connect, Error
from test import file_paths
import uvicorn
from datetime import datetime, timedelta
import os
import sys

# def check_and_process_file():
#     file_name = "new.txt"
#     try:
#         # Check if file exists
#         if os.path.isfile(file_name):
#             with open(file_name, 'r') as file:
#                 contents = file.read().strip()  # Read and strip whitespace for an exact match
#                 # Check if contents match "Ank"
#                 if contents == "Ankura@60":
#                     print("File exists and content is correct. Continuing execution...")
#                     return True
#                 else:
#                     print("File content does not match. Deleting file and exiting...")
#                     os.remove(file_name)
#         else:
#             print("File does not exist. Exiting...")
#     except Exception as e:
#         print(f"Error processing file: {e}")
#     # In case of file absence, mismatch, or error, use sys.exit() to stop execution
#     sys.exit()

app = FastAPI()

def collect_pg_data(start_date: str, end_date: str):
    table_paths = file_paths()
    print(start_date, end_date)
    try:
        # Convert start_date and end_date from string to datetime objects
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

        # Add one day to end_date
        end_date_plus_1_dt = end_date_dt + timedelta(days=1)

        # Convert back to string if necessary (here assuming the date format as YYYY-MM-DD)
        end_date_plus_1 = end_date_plus_1_dt.strftime('%Y-%m-%d')

        connection = connect(
            user="postgres",
            password="syscom",
            host="localhost",
            port="5432",
            database="postgres"
        )
        cursor = connection.cursor()

        cursor.execute("""
            SELECT *
            FROM wdtest
            WHERE punchdate::date >= %s AND punchdate::date <= %s
            ORDER BY punch_date_time ASC
        """, (start_date, end_date_plus_1))

        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        punches_from_postgres = pd.DataFrame(rows, columns=column_names)
        print(punches_from_postgres)

        punches_from_postgres.to_csv(table_paths['wdtest_path'], index=False)

    except (Exception, Error) as error:
        print("Error:", error)
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

@app.get("/collect_data/")
async def get_collect_data(start_date: str, end_date: str):
    collect_pg_data(start_date, end_date)
    df = pd.read_csv('wdtest.csv')  # Use the path from file_paths() instead of hardcoding
    return df.to_dict(orient="records")

if __name__ == "__main__":
    # check_and_process_file()
    uvicorn.run(app, host="0.0.0.0", port=9876, reload=False)
