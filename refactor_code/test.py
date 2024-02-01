from dbfread import DBF
import os
import pandas as pd

def test_db_len():
    dated_dbf = './dated.dbf'
    dated_table = DBF(dated_dbf, load=False) 
    dated_num_records = len(dated_table)

    muster_dbf = './muster.dbf'
    muster_table = DBF(muster_dbf, load=False) 
    muster_num_records = len(muster_table)

    holmast_dbf = './holmast.dbf'
    holmast_table = DBF(holmast_dbf, load=False) 
    holmast_num_records = len(holmast_table)

    punches_dbf = './punches.dbf'
    punches_table = DBF(punches_dbf, load=False) 
    punches_num_records = len(punches_table)

    lvform_dbf = './lvform.dbf'
    lvform_table = DBF(lvform_dbf, load=False) 
    lvform_num_records = len(lvform_table)

    with open('./empty_tables.txt', 'w') as file:
        if dated_num_records == 0:
            file.write("Blank dated table\n")

        if muster_num_records == 0:
            file.write("Blank muster table\n")

        if holmast_num_records == 0:
            file.write("Blank holmast table\n")

        if punches_num_records == 0:
            file.write("Blank punches table\n")

        if lvform_num_records == 0:
            file.write("Blank lvform table\n")

    if dated_num_records != 0 and muster_num_records != 0 and holmast_num_records != 0 and punches_num_records !=0 and lvform_num_records != 0:
        return 1
    else:
        return 0
    
def delete_old_files(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f'{file_path} deleted successfully')
    else:
        print(f'{file_path} does not exist')

def punch_mismatch():
    dated_dbf = './dated.dbf'
    dated_table = DBF(dated_dbf, load=True) 
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']

    punches_dbf = './punches.dbf'
    punches_table = DBF(punches_dbf, load=True)
    punches_df = pd.DataFrame(iter(punches_table))
    punches_df = punches_df[(punches_df['PDATE'] >= start_date) & (punches_df['PDATE'] <= end_date)]
    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:00')
    punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    mismatch_status = False 
    mask = punches_df['MODE'].eq(0) & punches_df['MODE'].shift(-1).eq(0)
    mismatch_df = punches_df[mask]
    mismatch_df = mismatch_df[['TOKEN', 'COMCODE', 'PDATE', 'MODE', 'PDTIME']]
    if len(mismatch_df) > 0:
        mismatch_status = True
        mismatch_df.to_csv('./mismatch.csv', index=False)

    if not mismatch_status:
        return 1
    else:
        return 0