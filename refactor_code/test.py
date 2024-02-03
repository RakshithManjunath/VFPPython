from dbfread import DBF
import os
import pandas as pd

def file_paths():
    ## common paths
    empty_tables_path = './empty_tables.txt'
    mismatch_csv_path = './mismatch.csv'

    new_txt_path = './new.txt'
    muster_csv_path = './muster.csv'

    punch_csv_path = './punch.csv'
    final_csv_path = './final.csv'

    ## normal execution
    # root_folder = 'D:/ZIONtest/'
    # dated_dbf = root_folder + 'dated.dbf'
    # muster_dbf = root_folder + 'muster.dbf'
    # holmast_dbf = root_folder + 'holmast.dbf'
    # punches_dbf = root_folder + 'punches.dbf'
    # lvform_dbf = root_folder + 'lvform.dbf'
    # exe = False
    # gsel_date_path = root_folder + 'gseldate.txt'

    ## exe
    dated_dbf = './dated.dbf'
    muster_dbf = './muster.dbf'
    holmast_dbf = './holmast.dbf'
    punches_dbf = './punches.dbf'
    lvform_dbf = './lvform.dbf'
    exe = True
    gsel_date_path = 'gseldate.txt'

    return {"dated_dbf_path":dated_dbf,
            "muster_dbf_path":muster_dbf,
            "holmast_dbf_path":holmast_dbf,
            "punches_dbf_path":punches_dbf,
            "lvform_dbf_path":lvform_dbf,
            "exe":exe,
            "empty_tables_path":empty_tables_path,
            "mismatch_csv_path":mismatch_csv_path,
            "new_txt_path":new_txt_path,
            "muster_csv_path":muster_csv_path,
            "punch_csv_path":punch_csv_path,
            "final_csv_path":final_csv_path,
            "gsel_date_path":gsel_date_path}

def check_ankura():
    table_paths = file_paths()
    if table_paths['exe'] == True:
        with open(table_paths['new_txt_path']) as f:
            if f.read() == "Ankura@60":
                pass
        os.remove(table_paths['new_txt_path'])

def test_db_len():
    table_paths = file_paths()
    dated_dbf = table_paths['dated_dbf_path']
    dated_table = DBF(dated_dbf, load=False) 
    dated_num_records = len(dated_table)

    muster_dbf = table_paths['muster_dbf_path']
    muster_table = DBF(muster_dbf, load=False) 
    muster_num_records = len(muster_table)

    holmast_dbf = table_paths['holmast_dbf_path']
    holmast_table = DBF(holmast_dbf, load=False) 
    holmast_num_records = len(holmast_table)

    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=False) 
    punches_num_records = len(punches_table)

    lvform_dbf = table_paths['lvform_dbf_path']
    lvform_table = DBF(lvform_dbf, load=False) 
    lvform_num_records = len(lvform_table)

    with open(table_paths['empty_tables_path'], 'w') as file:
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
       
def make_blank_files(dbf_path,columns=None):
    if os.path.exists(dbf_path) and columns is not None:
        df = pd.DataFrame(columns=columns)
        df.to_csv(dbf_path,index=False)

    elif os.path.exists(dbf_path):
        with open(dbf_path, 'w'):  # Open the file in write mode to truncate it
            pass  # Truncate the file, removing its contents
        print(f'Contents of {dbf_path} removed successfully')
    else:
        print(f'{dbf_path} does not exist')

def punch_mismatch():
    table_paths = file_paths()
    dated_dbf = table_paths['dated_dbf_path']
    dated_table = DBF(dated_dbf, load=True) 
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']

    muster_dbf = table_paths['muster_dbf_path']
    muster_table = DBF(muster_dbf, load=True)
    muster_df = pd.DataFrame(iter(muster_table))

    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=True)
    punches_df = pd.DataFrame(iter(punches_table))
    punches_df = punches_df[(punches_df['PDATE'] >= start_date) & (punches_df['PDATE'] <= end_date)]
    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:00')
    punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    mismatch_status = False 
    mask = punches_df['MODE'].eq(0) & punches_df['MODE'].shift(-1).eq(0)
    mismatch_df = punches_df[mask]
    mismatch_df = mismatch_df[['TOKEN','PDATE', 'MODE', 'PDTIME']]
    print(mismatch_df)
    if not mismatch_df.empty:
        result_df = pd.merge(mismatch_df, muster_df, on='TOKEN', how='right')
        result_df = result_df[['TOKEN','EMPCODE','NAME','COMCODE','PDATE','MODE','PDTIME']]
        mismatch_status = True
        result_df.to_csv(table_paths['mismatch_csv_path'], index=False)

    if not mismatch_status:
        return 1
    else:
        return 0