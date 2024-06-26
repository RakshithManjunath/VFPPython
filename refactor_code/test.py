from dbfread import DBF
import os
import pandas as pd
import requests
import shutil

def file_paths():
    ## common paths
    empty_tables_path = './empty_tables.txt'
    mismatch_csv_path = './mismatch.csv'

    new_txt_path = './new.txt'
    muster_csv_path = './muster.csv'

    punch_csv_path = './punch.csv'
    final_csv_path = './final.csv'

    payroll_input_path = 'payroll_input.csv'

    muster_role_path = 'muster_role.csv'

    ## normal execution
    # root_folder = 'D:/SWEETTOS/'
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
    # original_punches_path = root_folder + 'original_punches.csv'
    # day_one_out_excluded_path = root_folder + 'day_one_out_excluded.csv'
    # orphaned_punches_path = root_folder + 'orphaned_punches.csv'
    # out_of_range_punches_path = root_folder + 'out_of_range_punches.csv'
    # punches_full_len_df_path = root_folder + 'punches_full_len_df.csv'


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
    original_punches_path = './original_punches.csv'
    day_one_out_excluded_path = './day_one_out_excluded.csv'
    orphaned_punches_path = './orphaned_punches.csv'
    out_of_range_punches_path = './out_of_range_punches.csv'
    punches_full_len_df_path = './punches_full_len_df.csv'

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
            "original_punches_path":original_punches_path,
            "day_one_out_excluded_path":day_one_out_excluded_path,
            "orphaned_punches_path":orphaned_punches_path,
            "out_of_range_punches_path":out_of_range_punches_path,
            "punches_full_len_df_path":punches_full_len_df_path}

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
    muster_df = muster_df[muster_df['SEC_STAFF']==True]
    print("mismatch muster_df ", muster_df['TOKEN'])

    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=True)
    punches_df = pd.DataFrame(iter(punches_table))
    punches_df = punches_df[(punches_df['PDATE'] >= start_date) & (punches_df['PDATE'] <= end_date)]
    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    punches_df.to_csv(table_paths['original_punches_path'],index=False)

    not_modifed_punches_df = pd.DataFrame(iter(punches_table))
    not_satisfying_condition_df = not_modifed_punches_df[~((not_modifed_punches_df['PDATE'] >= start_date) & (not_modifed_punches_df['PDATE'] <= end_date))]
    not_satisfying_condition_df['PDTIME'] = pd.to_datetime(not_satisfying_condition_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    not_satisfying_condition_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    not_satisfying_condition_df.to_csv(table_paths['out_of_range_punches_path'],index=False)

    punches_full_len_df = pd.DataFrame(iter(punches_table))
    print("punches table:", len(punches_table))
    punches_full_len_df.to_csv(table_paths['punches_full_len_df_path'],index=False)

    raw_punches_merged_muster_df = pd.merge(punches_df, muster_df, on='TOKEN', how='inner')
    raw_punches_merged_muster_df = raw_punches_merged_muster_df[['TOKEN','EMPCODE','NAME','COMCODE_y','EMP_CO_COD','PDATE','MODE','PDTIME']]
    raw_punches_merged_muster_df['PDTIME'] = pd.to_datetime(raw_punches_merged_muster_df['PDTIME'])
    raw_punches_merged_muster_df['PDTIME'] = raw_punches_merged_muster_df['PDTIME'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
    raw_punches_merged_muster_df = raw_punches_merged_muster_df.rename(columns={'COMCODE_y': 'COMCODE'})
    raw_punches_merged_muster_df.to_csv('raw_punches_merged_muster_df.csv',index=False)

    agg_df = raw_punches_merged_muster_df.groupby('EMP_CO_COD')['MODE'].value_counts().unstack(fill_value=0).rename(columns={0: 'MODE_0_COUNT', 1: 'MODE_1_COUNT'})
    agg_df = agg_df.reset_index()

    agg_df['DIFFERENCE'] = abs(agg_df['MODE_0_COUNT'] - agg_df['MODE_1_COUNT'])

    df_difference_greater_than_0 = agg_df[agg_df['DIFFERENCE'] > 0]
    mismatch_df = raw_punches_merged_muster_df[raw_punches_merged_muster_df['EMP_CO_COD'].isin(df_difference_greater_than_0['EMP_CO_COD'])]
    print("before gseldate", mismatch_df.shape)

    df_difference_equal_0 = agg_df[agg_df['DIFFERENCE'] == 0]
    passed_df = raw_punches_merged_muster_df[raw_punches_merged_muster_df['EMP_CO_COD'].isin(df_difference_equal_0['EMP_CO_COD'])]

    result_passed_df = pd.merge(passed_df, muster_df, on='EMP_CO_COD', how='inner')
    result_passed_df = result_passed_df[['TOKEN_y','EMPCODE_y','NAME_y','COMCODE_y','PDATE','MODE','PDTIME']]
    result_passed_df['PDTIME'] = pd.to_datetime(result_passed_df['PDTIME'])
    result_passed_df['PDTIME'] = result_passed_df['PDTIME'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
    result_passed_df = result_passed_df.rename(columns={'COMCODE_y': 'COMCODE', 'TOKEN_y': 'TOKEN',
                                                        'EMPCODE_y': 'EMPCODE', 'NAME_y': 'NAME'})
    result_passed_df.to_csv(table_paths['passed_csv_path'],index=False)

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gsel_datetime = pd.to_datetime(gseldate)
        print(gseldate, type(gseldate))

    agg_df = mismatch_df.groupby('EMP_CO_COD')['MODE'].value_counts().unstack(fill_value=0).rename(columns={0: 'MODE_0_COUNT', 1: 'MODE_1_COUNT'})
    agg_df = agg_df.reset_index()

    agg_df['DIFFERENCE'] = abs(agg_df['MODE_0_COUNT'] - agg_df['MODE_1_COUNT'])

    df_difference_greater_than_0 = agg_df[agg_df['DIFFERENCE'] > 0]

    mismatch_df = raw_punches_merged_muster_df[raw_punches_merged_muster_df['EMP_CO_COD'].isin(df_difference_greater_than_0['EMP_CO_COD'])]
    print("before gseldate", mismatch_df.shape)
    mismatch_df['PDTIME'] = pd.to_datetime(mismatch_df['PDTIME'])

    if ((mismatch_df['MODE'] == 0) & (mismatch_df['PDTIME'].dt.date == gsel_datetime.date())).any():
        mismatch_status = False

    if len(mismatch_df) > 0:

        if start_date <= gsel_datetime <= end_date:
            gseldate_exclude_df = mismatch_df[mismatch_df['PDTIME'].dt.date == gsel_datetime.date()]

            result_gseldate_exclude_df = pd.merge(gseldate_exclude_df, muster_df, on='EMP_CO_COD', how='inner')
            result_gseldate_exclude_df = result_gseldate_exclude_df[['TOKEN_y','EMPCODE_y','NAME_y','COMCODE_y','PDATE','MODE','PDTIME']]
            result_gseldate_exclude_df['PDTIME'] = pd.to_datetime(result_gseldate_exclude_df['PDTIME'])
            result_gseldate_exclude_df['PDTIME'] = result_gseldate_exclude_df['PDTIME'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
            result_gseldate_exclude_df = result_gseldate_exclude_df.rename(columns={'COMCODE_y': 'COMCODE', 'TOKEN_y': 'TOKEN',
                                                        'EMPCODE_y': 'EMPCODE', 'NAME_y': 'NAME'})


            mismatch_df = mismatch_df[mismatch_df['PDTIME'].dt.date != gsel_datetime.date()]

        result_df = pd.merge(mismatch_df, muster_df, on='EMP_CO_COD', how='inner')
        result_df = result_df[['TOKEN_y','EMPCODE_y','NAME_y','COMCODE_y','PDATE','MODE','PDTIME']]
        mismatch_status = True
        result_df['PDTIME'] = pd.to_datetime(result_df['PDTIME'])
        result_df['PDTIME'] = result_df['PDTIME'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
        result_df = result_df.rename(columns={'COMCODE_y': 'COMCODE', 'TOKEN_y': 'TOKEN',
                                                        'EMPCODE_y': 'EMPCODE', 'NAME_y': 'NAME'})
            
        first_rows = result_df.groupby('TOKEN').first().reset_index()

        day_one_out_excluded = first_rows[first_rows['MODE'] == 1]

        day_one_out_excluded = pd.concat([day_one_out_excluded, result_gseldate_exclude_df], ignore_index=True)
        day_one_out_excluded.to_csv(table_paths['day_one_out_excluded_path'], index=False)

        result_df = result_df[~result_df.apply(tuple, 1).isin(day_one_out_excluded.apply(tuple, 1))]
        result_df.to_csv(table_paths['mismatch_csv_path'],index=False)

        passed_df_len = result_passed_df.shape[0]
        print("passed csv len: ",passed_df_len)
        day_one_out_excluded_df_len = day_one_out_excluded.shape[0]
        print("day one out excluded df len: ",day_one_out_excluded_df_len)
        result_df_len = result_df.shape[0]
        print("mismatch df len: ",result_df_len)
        punches_df_len = raw_punches_merged_muster_df.shape[0]
        print("og punches df len: ",punches_df_len)
        not_satisfying_condition_df_len = not_satisfying_condition_df.shape[0]
        print("date out of range len: ",not_satisfying_condition_df_len)
        punches_full_len_df_len = punches_full_len_df.shape[0]
        print("punches full len: ",punches_full_len_df_len)

        # punches_merged_muster_df = pd.merge(punches_df, muster_df, on='TOKEN', how='left')
        # punches_merged_muster_df = punches_merged_muster_df[['TOKEN','EMPCODE','NAME','COMCODE_y','PDATE','MODE','PDTIME']]
        # punches_merged_muster_df['PDTIME'] = pd.to_datetime(punches_merged_muster_df['PDTIME'])
        # punches_merged_muster_df['PDTIME'] = punches_merged_muster_df['PDTIME'].dt.strftime('%Y-%m-%d %I:%M:%S %p')
        # punches_merged_muster_df = punches_merged_muster_df.rename(columns={'COMCODE_y': 'COMCODE'})

        concatenated_mismatch_dayone_passed = pd.concat([result_df, day_one_out_excluded, result_passed_df])

        if (passed_df_len + day_one_out_excluded_df_len + result_df_len) != punches_df_len:
            print('length mismatch between punches and muster')
            orphaned_df = raw_punches_merged_muster_df[~raw_punches_merged_muster_df.astype(str).apply(tuple, 1).isin(concatenated_mismatch_dayone_passed.astype(str).apply(tuple, 1))]
            orphaned_df.to_csv(table_paths['orphaned_punches_path'],index=False)
            orphaned_df_len = orphaned_df.shape[0]
            print("orphaned punches df len",orphaned_df_len)
        # day_one_out_excluded = result_df[(result_df['MODE'] == 1) & (pd.to_datetime(result_df['PDATE']).dt.day == 1)]
        # day_one_out_excluded = day_one_out_excluded.sort_values(by='PDTIME').drop_duplicates(subset=['TOKEN', 'EMPCODE', 'PDATE'], keep='first')
        # day_one_out_excluded.to_csv('day_one_out_excluded.csv',index=False)

        # result_df = result_df.drop(day_one_out_excluded.index)

        # agg_df = result_df.groupby('TOKEN')['MODE'].value_counts().unstack(fill_value=0).rename(columns={0: 'MODE_0_COUNT', 1: 'MODE_1_COUNT'})
        # agg_df = agg_df.reset_index()

        # agg_df['DIFFERENCE'] = abs(agg_df['MODE_0_COUNT'] - agg_df['MODE_1_COUNT'])

        # df_difference_greater_than_0 = agg_df[agg_df['DIFFERENCE'] > 0]

        # result_df = result_df[punches_df['TOKEN'].isin(df_difference_greater_than_0['TOKEN'])]
        # print("before gseldate", result_df.shape)
        
        # result_df.to_csv(table_paths['mismatch_csv_path'], index=False)

    if mismatch_status == True:
        return 1,result_df
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

    