from dbfread import DBF
from dbf_handler import dbf_2_df
import os
import pandas as pd
import requests
import shutil
from dbf import Table,READ_WRITE

def file_paths():
    ## common paths
    new_txt_path = './new.txt'

    ## normal execution
    # root_folder = 'D:/ZIONtest/'
    # dated_dbf = root_folder + 'dated.dbf'
    # muster_dbf = root_folder + 'muster.dbf'
    # holmast_dbf = root_folder + 'holmast.dbf'
    # punches_dbf = root_folder + 'punches.dbf'
    # lvform_dbf = root_folder + 'lvform.dbf'
    # exe = False
    # gsel_date_path = root_folder + 'gseldate.txt'
    # g_option_path = root_folder + 'g_option.txt'
    # wdtest_path = root_folder + 'wdtest.csv'
    # wdtest_server_path = root_folder + 'wdtest_server.csv'
    # wdtest_client_path = root_folder + 'wdtest_client.csv'
    # passed_csv_path = root_folder + 'passed.csv'
    # day_one_out_excluded_path = root_folder + 'day_one_out_punches.csv'
    # orphaned_punches_path = root_folder + 'orphaned_punches.csv'
    # out_of_range_punches_path = root_folder + 'out_of_range_punches.csv'
    # gsel_date_excluded_punches_len_df_path = root_folder + 'gsel_date_punches.csv'
    # holmast_csv_path = root_folder + 'holiday.csv'
    # lvform_csv_path = root_folder + 'leave.csv'
    # pytotpun_dbf_path = root_folder + 'pytotpun.dbf'
    # mismatch_report_path = root_folder + 'mismatch_report.csv'
    # passed_punches_df_path = root_folder + 'passed_punches.csv'
    # mismatch_punches_df_path = root_folder + 'mismatch_punches.csv'
    # total_punches_df_path = root_folder + 'total_punches.csv'

    # empty_tables_path = root_folder + 'empty_tables.txt'
    # mismatch_csv_path = root_folder + 'mismatch.csv'

    # muster_csv_path = root_folder + 'muster.csv'

    # punch_csv_path = root_folder + 'punch.csv'
    # final_csv_path = root_folder + 'final.csv'

    # payroll_input_path = root_folder + 'payroll_input.csv'

    # muster_role_path = root_folder + 'muster_role.csv'

    ## exe
    root_folder = './'
    dated_dbf = './dated.dbf'
    muster_dbf = './muster.dbf'
    holmast_dbf = './holmast.dbf'
    punches_dbf = './punches.dbf'
    lvform_dbf = './lvform.dbf'
    exe = True
    gsel_date_path = './gseldate.txt'
    g_option_path = './g_option.txt'
    wdtest_path = './wdtest.csv'
    wdtest_server_path = './wdtest_server.csv'
    wdtest_client_path = './wdtest_client.csv'
    passed_csv_path = './passed.csv'
    day_one_out_excluded_path = './day_one_out_punches.csv'
    orphaned_punches_path = './orphaned_punches.csv'
    out_of_range_punches_path = './out_of_range_punches.csv'
    gsel_date_excluded_punches_len_df_path = './gsel_date_punches.csv'
    holmast_csv_path = './holiday.csv'
    lvform_csv_path = './leave.csv'
    pytotpun_dbf_path = './pytotpun.dbf'
    mismatch_report_path = './mismatch_report.csv'
    passed_punches_df_path = './passed_punches.csv'
    mismatch_punches_df_path = './mismatch_punches.csv'
    total_punches_df_path = './total_punches.csv'

    empty_tables_path = './empty_tables.txt'
    mismatch_csv_path = './mismatch.csv'

    muster_csv_path = './muster.csv'

    punch_csv_path = './punch.csv'
    final_csv_path = './final.csv'

    payroll_input_path = './payroll_input.csv'

    muster_role_path = './muster_role.csv'

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
            "gsel_date_path":gsel_date_path,
            "g_option_path":g_option_path,
            "payroll_input_path":payroll_input_path,
            "wdtest_path":wdtest_path,
            "wdtest_server_path":wdtest_server_path,
            "wdtest_client_path":wdtest_client_path,
            "root_folder":root_folder,
            "muster_role_path":muster_role_path,

            "passed_csv_path":passed_csv_path,
            "day_one_out_excluded_path":day_one_out_excluded_path,
            "orphaned_punches_path":orphaned_punches_path,
            "out_of_range_punches_path":out_of_range_punches_path,
            "gsel_date_excluded_punches_len_df_path":gsel_date_excluded_punches_len_df_path,
            
            "holmast_csv_path":holmast_csv_path,
            "lvform_csv_path":lvform_csv_path,
            "pytotpun_dbf_path":pytotpun_dbf_path,
            "mismatch_report_path":mismatch_report_path,
            
            "passed_punches_df_path":passed_punches_df_path,
            "mismatch_punches_df_path":mismatch_punches_df_path,
            "total_punches_df_path":total_punches_df_path}

def check_ankura():
    table_paths = file_paths()
    print("Exe status: ", table_paths['exe'])
    if table_paths['exe'] == True:
        with open(table_paths['new_txt_path']) as f:
            if f.read() == "Ankura@60":
                pass
        os.remove(table_paths['new_txt_path'])

def check_database():
    table_paths = file_paths()
    with open(table_paths['g_option_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        g_process_mode,g_pgdata = int(file_contents[1]), int(file_contents[0])
    return g_pgdata, g_process_mode

def test_db_len():
    table_paths = file_paths()
    dated_dbf = table_paths['dated_dbf_path']
    dated_num_records = dbf_2_df(filename=dated_dbf,type="len")
    # dated_table = DBF(dated_dbf, load=False) 
    # dated_num_records = len(dated_table)

    muster_dbf = table_paths['muster_dbf_path']
    muster_num_records = dbf_2_df(filename=muster_dbf,type="len")
    # muster_table = DBF(muster_dbf, load=False) 
    # muster_num_records = len(muster_table)

    holmast_dbf = table_paths['holmast_dbf_path']
    holmast_num_records = dbf_2_df(filename=holmast_dbf,type="len")
    # holmast_table = DBF(holmast_dbf, load=False) 
    # holmast_num_records = len(holmast_table)

    punches_dbf = table_paths['punches_dbf_path']
    punches_num_records = dbf_2_df(filename=punches_dbf,type="len")
    # punches_table = DBF(punches_dbf, load=False) 
    # punches_num_records = len(punches_table)

    lvform_dbf = table_paths['lvform_dbf_path']
    lvform_num_records = dbf_2_df(filename=punches_dbf,type="len")
    # lvform_table = DBF(lvform_dbf, load=False) 
    # lvform_num_records = len(lvform_table)

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

    if dated_num_records != 0 and muster_num_records != 0 and punches_num_records !=0:
        if holmast_num_records ==0 or lvform_num_records == 0:
            return {'optional_tables': [holmast_num_records,lvform_num_records]}
        return {'required_tables': [dated_num_records,muster_num_records,punches_num_records]}
    else:
        return 0
       
def make_blank_files(dbf_path,columns=None):
    if os.path.exists(dbf_path) and columns is not None:
        df = pd.DataFrame(columns=columns)
        df.to_csv(dbf_path,index=False)

    elif os.path.exists(dbf_path):
        with open(dbf_path, 'w'):  
            pass  
        print(f'Contents of {dbf_path} removed successfully')
    else:
        print(f'{dbf_path} does not exist')

def create_new_csvs(muster_dbf_path,muster_columns,
                    punch_dbf_path,punch_columns,
                    final_dbf_path,final_columns):
    if os.path.exists(muster_dbf_path) == False:
        print(muster_dbf_path, "does not exist")
        df = pd.DataFrame(columns=muster_columns)
        df.to_csv(muster_dbf_path,index=False)

    if os.path.exists(punch_dbf_path) == False:
        print(punch_dbf_path, "does not exist")
        df = pd.DataFrame(columns=punch_columns)
        df.to_csv(punch_dbf_path,index=False)

    if os.path.exists(final_dbf_path) == False:
        print(final_dbf_path, "does not exist")
        df = pd.DataFrame(columns=final_columns)
        df.to_csv(final_dbf_path,index=False)

def delete_old_files(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f'{file_path} deleted successfully')
    else:
        print(f'{file_path} does not exist')

def punch_mismatch():
    table_paths = file_paths()
    dated_dbf = table_paths['dated_dbf_path']
    dated_table = DBF(dated_dbf, load=True) 
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']

    muster_dbf = table_paths['muster_dbf_path']
    muster_table = DBF(muster_dbf, load=True)
    muster_df = pd.DataFrame(iter(muster_table))
    print("mismatch muster_df ", muster_df['TOKEN'])

    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=True)
    print("punches dbf length: ",len(punches_table))
    punches_df = pd.DataFrame(iter(punches_table))
    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    print(f"Before dropping duplicates: {punches_df.shape[0]}")

    pytotpun_dbf = table_paths['pytotpun_dbf_path']
    pytotpun_table = DBF(pytotpun_dbf, load=True)
    pytotpun_df = pd.DataFrame(iter(pytotpun_table))
    pytotpun_num_records = len(pytotpun_df)
    if pytotpun_num_records !=0:
        print('********* Making pymismatch as punches **********')
        punches_df = pytotpun_df
        pytotpun_df.to_csv(table_paths['total_punches_df_path'],index=False)
    elif pytotpun_num_records == 0:
        punches_df['PDATE'] = pd.to_datetime(punches_df['PDATE'])
        punches_df['PDATE'] = punches_df['PDATE'].dt.date
        table = Table(table_paths['pytotpun_dbf_path'])
        table.open(mode=READ_WRITE)

        for index, row in punches_df.iterrows():
            record = {field: row[field] for field in table.field_names if field in punches_df.columns}
            table.append(record)
        table.close()
        punches_df.to_csv(table_paths['total_punches_df_path'],index=False)

    # Identify columns to be considered for dropping duplicates (excluding 'MODE' and 'MCIP')
    columns_to_consider = punches_df.columns.difference(['TOKEN', 'COMCODE', 'PDTIME', 'MODE', 'MCIP'])

    # Find the duplicated rows based on the specified columns, keeping only the first occurrence
    duplicates = punches_df[punches_df.duplicated(subset=columns_to_consider, keep='first')]

    # Drop duplicates from the DataFrame based on the specified columns, keeping only the first occurrence
    punches_df = punches_df.drop_duplicates(subset=columns_to_consider, keep='first')

    # Display the number of rows after dropping duplicates
    print(f"After dropping duplicates: {punches_df.shape[0]}")

    # Display the rows that were removed
    print("Rows that were removed:")
    print(duplicates)

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gsel_datetime = pd.to_datetime(gseldate)
        print(gseldate, type(gseldate))

    out_of_range_punches_df = punches_df[~((punches_df['PDATE'] >= start_date) & (punches_df['PDATE'] <= end_date))]
    out_of_range_punches_df.to_csv(table_paths['out_of_range_punches_path'],index=False)
    # out_of_range_punches_df = out_of_range_punches_df.merge(punches_df, on='TOKEN', how='inner')
    # print('Out of range columns: ',out_of_range_punches_df.columns)
    # out_of_range_punches_df = out_of_range_punches_df[['TOKEN','COMCODE_y','PDATE_x','HOURS_x','MINUTES_x','MODE_x','PDTIME_x','MCIP_x']]
    # out_of_range_punches_df = out_of_range_punches_df.rename(columns={'COMCODE_y':'COMCODE','PDATE_x':'PDATE',
    #                                                       'HOURS_x':'HOURS','MINUTES_x':'MINUTES',
    #                                                       'MODE_x':'MODE','PDTIME_x':'PDTIME','MCIP_x':'MCIP'})
    # print(f"Out of range punches: {out_of_range_punches_df.shape[0]} {out_of_range_punches_df}")
    # out_of_range_punches_df.to_csv(table_paths['out_of_range_punches_path'],index=False)

    merged_df = punches_df.merge(out_of_range_punches_df, on=['TOKEN','PDTIME','MODE'], how='inner')
    punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    print(f"After removing out of range punches: {punches_df.shape[0]}")

    merged_orphaned_df = punches_df.merge(muster_df, on='TOKEN', how='outer', indicator=True)
    orphaned_df = merged_orphaned_df[merged_orphaned_df['_merge'] == 'left_only']
    orphaned_df = orphaned_df.drop(columns=['_merge'])
    print("Orphaned columns: ",orphaned_df.columns)
    orphaned_punches_df = orphaned_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    orphaned_punches_df = orphaned_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
    orphaned_punches_df.to_csv(table_paths['orphaned_punches_path'],index=False)

    merged_df = punches_df.merge(orphaned_punches_df, on=['TOKEN', 'PDTIME','MODE'], how='inner')
    punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    print(f"After removing orphaned punches: {punches_df.shape[0]}")

    day_one_excluded = pd.merge(punches_df, muster_df, on='TOKEN', how='inner')
    day_one_excluded = day_one_excluded[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    print('day one out all columns: ',day_one_excluded.columns)
    day_one_excluded['PDTIME'] = pd.to_datetime(day_one_excluded['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    day_one_excluded = day_one_excluded.rename(columns={'COMCODE_y': 'COMCODE'})
    first_rows = day_one_excluded.groupby('TOKEN').first().reset_index()
    day_one_out_excluded_df = first_rows[first_rows['MODE'] == 1]
    day_one_out_excluded_df.to_csv(table_paths['day_one_out_excluded_path'], index=False)
    print(f"Day one out excluded: {day_one_out_excluded_df.shape[0]} {day_one_out_excluded_df}")

    merged_df = punches_df.merge(day_one_out_excluded_df, on=['TOKEN', 'PDTIME','MODE'], how='inner')
    punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    print(f"After removing day one out punches: {punches_df.shape[0]}")

    print(start_date, type(start_date), gsel_datetime.date(), type(gsel_datetime.date()), end_date, type(end_date))

    if start_date <= gsel_datetime.date() <= end_date:
        gseldate_exclude_df = punches_df[(punches_df['PDTIME'].dt.date == gsel_datetime.date())]

        result_gseldate_exclude_df = pd.merge(gseldate_exclude_df, muster_df, on='TOKEN', how='inner')
        result_gseldate_exclude_df = result_gseldate_exclude_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
        print('gseldate exclude columns: ',result_gseldate_exclude_df.columns)
        result_gseldate_exclude_df['PDTIME'] = pd.to_datetime(result_gseldate_exclude_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
        result_gseldate_exclude_df = result_gseldate_exclude_df.rename(columns={'COMCODE_y': 'COMCODE'})
        result_gseldate_exclude_df.to_csv(table_paths['gsel_date_excluded_punches_len_df_path'], index=False)
        print(f"Gsel date excluded: {result_gseldate_exclude_df.shape[0]} {result_gseldate_exclude_df}")

        merged_df = punches_df.merge(result_gseldate_exclude_df, on=['TOKEN', 'PDTIME','MODE'], how='inner')
        print("merged df columns: ",merged_df.columns)
        punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
        print(f"After removing gsel date excluded punches: {punches_df.shape[0]}")

        punches_df = pd.concat([punches_df, result_gseldate_exclude_df, day_one_out_excluded_df], ignore_index=True)
        punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        # punches_df.to_csv('merged_punches_dayone_gsel.csv',index=False)

    def is_alternating(sequence):
        return all(sequence[i] != sequence[i + 1] for i in range(len(sequence) - 1))

    passed_list = []
    mismatch_list = []

    for token, group in punches_df.groupby('TOKEN'):
        mode_sequence = group['MODE'].tolist()
        
        if mode_sequence[0] == 0 and is_alternating(mode_sequence):
            passed_list.append(group)
        else:
            mismatch_list.append(group)

    mismatch = pd.concat(mismatch_list)

    passed = punches_df[~punches_df['TOKEN'].isin(mismatch['TOKEN'].unique())]

    result_passed_df = pd.merge(passed, muster_df, on='TOKEN', how='inner')
    print("result passed df cols: ",result_passed_df.columns)

    passed_punches_df = result_passed_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    passed_punches_df = passed_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
    passed_punches_df.to_csv(table_paths['passed_punches_df_path'],index=False)

    mismatch['PDTIME'] = pd.to_datetime(mismatch['PDTIME'])

    if ((mismatch['MODE'] == 0) & (mismatch['PDTIME'].dt.date == gsel_datetime.date())).any():
        mismatch_status = False

    result_mismatch_df = pd.merge(mismatch, muster_df, on='TOKEN', how='inner')
    print("Mismatch punches columns: ",result_mismatch_df.columns)

    mismatch_punches_df = result_mismatch_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    mismatch_punches_df = mismatch_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
    mismatch_punches_df.to_csv(table_paths['mismatch_punches_df_path'],index=False)
    mismatch_status = True

    mismatch_for_editing = pd.concat([mismatch_punches_df,result_gseldate_exclude_df,day_one_out_excluded_df], ignore_index=True)
    mismatch_for_editing_merged_with_muster = pd.merge(mismatch_for_editing, muster_df, on='TOKEN', how='inner')
    mismatch_for_editing_with_name = mismatch_for_editing_merged_with_muster[['TOKEN','NAME','EMPCODE']]
    mismatch_for_editing_with_name = mismatch_for_editing_with_name.rename(columns={'COMCODE_y':'COMCODE'})
    mismatch_for_editing_with_name = mismatch_for_editing_with_name.drop_duplicates()
    if len(mismatch_for_editing_with_name) !=0:
        mismatch_for_editing_with_name.to_csv(table_paths['mismatch_report_path'],index=False)


    pytotpun_df = pd.concat([passed_punches_df,mismatch_punches_df,result_gseldate_exclude_df,day_one_out_excluded_df], ignore_index=True)

    pytotpun_df['PDATE'] = pd.to_datetime(pytotpun_df['PDATE'])
    pytotpun_df['PDATE'] = pytotpun_df['PDATE'].dt.date

    table = Table(table_paths['pytotpun_dbf_path'])
    table.open(mode=READ_WRITE)
    table.zap()

    for index, row in pytotpun_df.iterrows():
        record = {field: row[field] for field in table.field_names if field in pytotpun_df.columns}
        table.append(record)
    table.close()

    passed_df_len = result_passed_df.shape[0]
    print("passed csv len: ",passed_df_len)
    mismatch_df_len = result_mismatch_df.shape[0]
    print("mismatch df len: ",mismatch_df_len)

    if mismatch_status == True:
        return 1,result_mismatch_df,punches_df
    else:
        return 1,None
    
def server_collect_db_data():
    table_paths = file_paths()
    with open(table_paths['g_option_path']) as file:
        data_collect_flag = file.readline().strip()
        data_process_flag = file.readline().strip()
        ip_address = file.readline().strip( )
        start_date = file.readline().strip()
        end_date = file.readline().strip()

    url = f"http://{ip_address}:9876/collect_data/?start_date={start_date}&end_date={end_date}"
    response = requests.get(url)

    df = pd.DataFrame(response.json())

    columns_to_drop = ['punchtime']
    columns_to_rename = {'empcode': 'TOKEN', 'punchdate': 'PDATE', 'punch_date_time': 'PDTIME', 'terminal_sl': 'MCIP'}

    columns_to_drop = [column for column in columns_to_drop if column in df.columns]

    df = df.drop(columns=columns_to_drop)

    columns_to_rename = {old: new for old, new in columns_to_rename.items() if old in df.columns}
    df = df.rename(columns=columns_to_rename)

    df['PDTIME'] = pd.to_datetime(df['PDTIME'])
    df['PDTIME'] = df['PDTIME'].apply(lambda dt: dt.replace(second=0))

    df['PDATE'] = df['PDATE'].astype(str)

    df['TOKEN'] = df['TOKEN'].astype(str)

    new_order = ['TOKEN','PDATE','PDTIME','MCIP']

    df = df[new_order]

    print('*********')
    print("server df dtypes", df.dtypes)
    print("server", os.getcwd())
    df.to_csv(table_paths['wdtest_server_path'],index=False)
    
    return df

def client_collect_db_data():
    table_paths = file_paths()
    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=False)
    punches_num_records = len(punches_table)
    if punches_num_records == 0:
        df = pd.read_csv(table_paths['wdtest_server_path'])
        df.to_csv(table_paths['wdtest_path'],index=False)
        return None
    else:
        punches_table = DBF(table_paths['punches_dbf_path'], load=True)
        punches_df = pd.DataFrame(iter(punches_table))
        punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
        punches_df['PDATE'] = punches_df['PDATE'].astype(str)
        punches_df['TOKEN'] = punches_df['TOKEN'].astype(str)
        print("punches dbf client",punches_df)

        columns_to_drop = ['COMCODE', 'HOURS', 'MINUTES', 'MODE']
        columns_to_drop = [column for column in columns_to_drop if column in punches_df.columns]

        punches_df = punches_df.drop(columns=columns_to_drop)
        print('*********')
        print("client df dtypes", punches_df.dtypes)
        
        punches_df.to_csv(table_paths['wdtest_client_path'],index=False)

        return punches_df

def create_wdtest(server_df,client_df):
    table_paths = file_paths()
    result_df = pd.merge(server_df, client_df, on=['TOKEN', 'PDATE', 'PDTIME', 'MCIP'], how='left', indicator=True)
    print("server", server_df.dtypes)
    print("client", client_df.dtypes)

    result_df = result_df[result_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    print("wdtest", os.getcwd())
    
    result_df.to_csv(table_paths['wdtest_path'],index=False)

    