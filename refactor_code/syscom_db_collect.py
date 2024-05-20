from fastapi import FastAPI, HTTPException
import pandas as pd
from psycopg2 import connect, Error
from test import file_paths
import uvicorn
from datetime import datetime, timedelta

app = FastAPI()

def connect_to_database():
    return connect(
        user="postgres",
        password="syscom",
        host="localhost",
        port="5432",
        database="postgres"
    )

def fetch_data_from_db(connection, start_date, end_date):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT *
            FROM wdtest
            WHERE punchdate::date >= %s AND punchdate::date <= %s
            ORDER BY punch_date_time ASC
        """, (start_date, end_date))
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=column_names)

def collect_pg_data(start_date: str, end_date: str):
    table_paths = file_paths()
    try:
        start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
        end_date_plus_1 = (end_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')

        connection = connect_to_database()
        punches_from_postgres = fetch_data_from_db(connection, start_date, end_date_plus_1)
        punches_from_postgres.to_csv(table_paths['wdtest_path'], index=False)
    except (Exception, Error) as error:
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")

@app.get("/collect_data/")
async def get_collect_data(start_date: str, end_date: str):
    collect_pg_data(start_date, end_date)
    table_paths = file_paths()
    df = pd.read_csv(table_paths['wdtest_path'])
    return df.to_dict(orient="records")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9876, reload=False)