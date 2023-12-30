from dbfread import DBF
import pandas as pd

muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)

must_token = [record['TOKEN'] for record in muster_table]
name = [record['NAME'] for record in muster_table]
employee_code = [record['EMPCODE'] for record in muster_table]
date_join = [record['DATE_JOIN'] for record in muster_table]
date_leave = [record['DATE_LEAVE'] for record in muster_table]

dated_table = DBF('D:/ZIONtest/dated.dbf', load=True)
start_date = dated_table.records[0]['MUFRDATE']
end_date = dated_table.records[0]['MUTODATE']

data = {"must_token": must_token,
        "name": name,
        "employee_code": employee_code,
        "date_join": date_join,
        "date_leave": date_leave,
        "new_start_date": start_date,
        "new_end_date": end_date}

data = pd.DataFrame(data)
data.to_csv('muster.csv', index=False)