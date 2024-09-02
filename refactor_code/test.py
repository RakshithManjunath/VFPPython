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
    # root_folder = 'D:/JPDSHIFT_Makali/'
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
    # orphaned_punches_path = root_folder + 'orphaned_punches.csv'
    # out_of_range_punches_path = root_folder + 'out_of_range_punches.csv'
    # holmast_csv_path = root_folder + 'holiday.csv'
    # lvform_csv_path = root_folder + 'leave.csv'
    # pytotpun_dbf_path = root_folder + 'pytotpun.dbf'
    # mismatch_report_path = root_folder + 'mismatch_report.csv'
    # passed_punches_df_path = root_folder + 'passed_punches.csv'
    # mismatch_punches_df_path = root_folder + 'mismatch_punches.csv'
    # total_punches_punches_df_path = root_folder + 'total_punches_punches.csv'
    # total_pytotpun_punches_df_path = root_folder + 'total_pytotpun_punches.csv'
    # actual_punches_df_path = root_folder + 'actual_punches.csv'
    # duplicate_punches_df_path = root_folder + 'duplicates_punches.csv'

    # empty_tables_path = root_folder + 'empty_tables.txt'
    # mismatch_csv_path = root_folder + 'mismatch.csv'

    # muster_csv_path = root_folder + 'muster.csv'

    # punch_csv_path = root_folder + 'punch.csv'
    # final_csv_path = root_folder + 'final.csv'

    # payroll_input_path = root_folder + 'payroll_input.csv'

    # muster_role_path = root_folder + 'muster_role.csv'

    # gsel_date_excluded_punches_len_df_path = root_folder + 'gseldate_punches.csv'

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
    orphaned_punches_path = './orphaned_punches.csv'
    out_of_range_punches_path = './out_of_range_punches.csv'
    holmast_csv_path = './holiday.csv'
    lvform_csv_path = './leave.csv'
    pytotpun_dbf_path = './pytotpun.dbf'
    mismatch_report_path = './mismatch_report.csv'
    passed_punches_df_path = './passed_punches.csv'
    mismatch_punches_df_path = './mismatch_punches.csv'
    total_punches_punches_df_path = './total_punches_punches.csv'
    total_pytotpun_punches_df_path = './total_pytotpun_punches.csv'
    actual_punches_df_path = './actual_punches.csv'
    duplicate_punches_df_path = './duplicates_punches.csv'

    empty_tables_path = './empty_tables.txt'
    mismatch_csv_path = './mismatch.csv'

    muster_csv_path = './muster.csv'

    punch_csv_path = './punch.csv'
    final_csv_path = './final.csv'

    payroll_input_path = './payroll_input.csv'

    muster_role_path = './muster_role.csv'

    gsel_date_excluded_punches_len_df_path = './gseldate_punches.csv'

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
            "orphaned_punches_path":orphaned_punches_path,
            "out_of_range_punches_path":out_of_range_punches_path,
            
            "holmast_csv_path":holmast_csv_path,
            "lvform_csv_path":lvform_csv_path,
            "pytotpun_dbf_path":pytotpun_dbf_path,
            "mismatch_report_path":mismatch_report_path,
            
            "passed_punches_df_path":passed_punches_df_path,
            "mismatch_punches_df_path":mismatch_punches_df_path,
            "total_punches_punches_df_path":total_punches_punches_df_path,
            "actual_punches_df_path":actual_punches_df_path,
            "duplicate_punches_df_path":duplicate_punches_df_path,
            "total_pytotpun_punches_df_path":total_pytotpun_punches_df_path,
            "gsel_date_excluded_punches_len_df_path":gsel_date_excluded_punches_len_df_path}

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

    holmast_csv = table_paths['holmast_csv_path']
    holmast_num_records = dbf_2_df(filename=holmast_csv,type="csv")
    # holmast_table = DBF(holmast_dbf, load=False) 
    # holmast_num_records = len(holmast_table)

    punches_dbf = table_paths['punches_dbf_path']
    punches_num_records = dbf_2_df(filename=punches_dbf,type="len")
    # punches_table = DBF(punches_dbf, load=False) 
    # punches_num_records = len(punches_table)

    lvform_csv = table_paths['lvform_csv_path']
    lvform_num_records = dbf_2_df(filename=lvform_csv,type="csv")
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
        pytotpun_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        punches_df = pytotpun_df
        pytotpun_df.to_csv(table_paths['total_pytotpun_punches_df_path'],index=False)
    elif pytotpun_num_records == 0:
        punches_df['PDATE'] = pd.to_datetime(punches_df['PDATE'])
        punches_df['PDATE'] = punches_df['PDATE'].dt.date
        table = Table(table_paths['pytotpun_dbf_path'])
        table.open(mode=READ_WRITE)

        for index, row in punches_df.iterrows():
            record = {field: row[field] for field in table.field_names if field in punches_df.columns}
            table.append(record)
        table.close()
        punches_df.to_csv(table_paths['total_punches_punches_df_path'],index=False)

    # Removing duplicates based on TOKEN, COMCODE, and PDTIME
    unique_punches_df = punches_df.drop_duplicates(subset=["TOKEN", "PDTIME"], keep='first')

    # Identifying and creating a DataFrame for the removed rows
    duplicates_removed_df = punches_df[~punches_df.index.isin(unique_punches_df.index)]
    duplicates_removed_df.to_csv(table_paths['duplicate_punches_df_path'],index=False)

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gsel_datetime = pd.to_datetime(gseldate)
        print(gseldate, type(gseldate))

    out_of_range_punches_df = unique_punches_df[~((unique_punches_df['PDATE'] >= start_date) & (unique_punches_df['PDATE'] <= end_date))]
    out_of_range_punches_df.to_csv(table_paths['out_of_range_punches_path'],index=False)

    merged_df = unique_punches_df.merge(out_of_range_punches_df, on=['TOKEN','PDTIME','MODE'], how='inner')
    punches_df = unique_punches_df[~unique_punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
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
    punches_df.to_csv(table_paths['actual_punches_df_path'],index=False)

    # if start_date <= gsel_datetime <= end_date:
    #     gseldate_exclude_df = punches_df[punches_df['PDTIME'].dt.date == gsel_datetime.date()]
    #     # gseldate_exclude_df = gseldate_exclude_df[gseldate_exclude_df['MODE'] == 0]

    #     result_gseldate_exclude_df = pd.merge(gseldate_exclude_df, muster_df, on='TOKEN', how='inner')
    #     result_gseldate_exclude_df = result_gseldate_exclude_df[['TOKEN','EMPCODE','NAME','COMCODE_y','PDATE','MODE','PDTIME']]
    #     result_gseldate_exclude_df['PDTIME'] = pd.to_datetime(result_gseldate_exclude_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    #     result_gseldate_exclude_df = result_gseldate_exclude_df.rename(columns={'COMCODE_y': 'COMCODE'})
    #     result_gseldate_exclude_df.to_csv(table_paths['gsel_date_excluded_punches_len_df_path'], index=False)
    #     print(f"Gsel date excluded: {result_gseldate_exclude_df.shape[0]} {result_gseldate_exclude_df}")

    #     merged_df = punches_df.merge(result_gseldate_exclude_df, on=['TOKEN', 'PDTIME','MODE'], how='inner')
    #     punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    #     print(f"After removing gsel date excluded punches: {punches_df.shape[0]}")

    # Function to check the 0101... pattern and count equality

    def check_pattern(group):
        modes = group['MODE'].tolist()
        pattern = [0, 1] * (len(modes) // 2) + [0] * (len(modes) % 2)
        count_0 = modes.count(0)
        count_1 = modes.count(1)
        return modes == pattern and count_0 == count_1

    # Separate into passed, mismatch, and gseldate_punches
    passed = pd.DataFrame(columns=punches_df.columns)
    mismatch = pd.DataFrame(columns=punches_df.columns)
    gseldate_punches = pd.DataFrame(columns=punches_df.columns)

    for _, group in punches_df.groupby(['TOKEN']):
        # Check if the pattern is correct
        if group.iloc[0]['MODE'] == 0 and check_pattern(group):
            passed = pd.concat([passed, group])
        else:
            # Check if the last mode is 0 and if it corresponds to the specific date
            if group.iloc[-1]['MODE'] == 0:
                print(f"Checking TOKEN: {group.iloc[0]['TOKEN']}, Last MODE is 0")
                if group.iloc[-1]['PDTIME'].date() == gsel_datetime.date():
                    print(f"Last row date matches gsel_datetime for TOKEN: {group.iloc[0]['TOKEN']}")
                    # Move the last row to gseldate_punches
                    gseldate_punches = pd.concat([gseldate_punches, group.iloc[[-1]]])
                    # Re-check the pattern for the remaining rows
                    remaining_group = group.iloc[:-1]
                    if check_pattern(remaining_group):
                        passed = pd.concat([passed, remaining_group])
                    else:
                        mismatch = pd.concat([mismatch, remaining_group])
                else:
                    print(f"Date does not match gsel_datetime for TOKEN: {group.iloc[0]['TOKEN']}")
                    mismatch = pd.concat([mismatch, group])
            else:
                print(f"Last MODE is not 0 for TOKEN: {group.iloc[0]['TOKEN']}")
                mismatch = pd.concat([mismatch, group])

    # Save the gseldate_punches to CSV
    gseldate_punches.to_csv(table_paths['gsel_date_excluded_punches_len_df_path'], index=False)

    # Add a new column 'Remarks'
    mismatch['REMARKS'] = ""

    # Function to check the pattern and stop after the first break
    def check_pattern_stop_on_first_break_v4(df):
        mode_counts = {0: 0, 1: 0}
        pattern_broken = False
        
        for i in range(1, len(df)):
            current_mode = df.iloc[i]['MODE']
            previous_mode = df.iloc[i-1]['MODE']
            
            # Update the mode counts
            mode_counts[current_mode] += 1
            
            # Check if the pattern is broken
            if current_mode == previous_mode and not pattern_broken:
                # Directly update the 'Remarks' column in the same row
                df.iloc[i, df.columns.get_loc('REMARKS')] = df.iloc[i]['PDTIME']
                pattern_broken = True  # Stop further checks after the first break
                break
                
        return df

    # Apply the pattern check for each TOKEN
    mismatch = mismatch.groupby('TOKEN', group_keys=False).apply(check_pattern_stop_on_first_break_v4)

    result_passed_df = pd.merge(passed, muster_df, on='TOKEN', how='inner')
    print("result passed df cols: ",result_passed_df.columns)

    passed_punches_df = result_passed_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    passed_punches_df = passed_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
    passed_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    passed_punches_df.to_csv(table_paths['passed_punches_df_path'],index=False)

    mismatch['PDTIME'] = pd.to_datetime(mismatch['PDTIME'])

    if ((mismatch['MODE'] == 0) & (mismatch['PDTIME'].dt.date == gsel_datetime.date())).any():
        mismatch_status = False

    result_mismatch_df = pd.merge(mismatch, muster_df, on='TOKEN', how='inner')
    print("Mismatch punches columns: ",result_mismatch_df.columns)

    mismatch_punches_df = result_mismatch_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
    mismatch_punches_df = mismatch_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
    mismatch_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    mismatch_punches_df.to_csv(table_paths['mismatch_punches_df_path'],index=False)

    mismatch_status = True

    # mismatch_for_editing = pd.concat([mismatch_punches_df,result_gseldate_exclude_df,day_one_out_excluded_df], ignore_index=True)
    mismatch_for_editing_merged_with_muster = pd.merge(mismatch, muster_df, on='TOKEN', how='inner')
    mismatch_for_editing_with_name = mismatch_for_editing_merged_with_muster[['TOKEN','NAME','EMPCODE','MODE']]
    mismatch_for_editing_with_name = mismatch_for_editing_with_name.rename(columns={'COMCODE_y':'COMCODE'})

    mismatch_for_editing_with_name['MODE_0_COUNT'] = mismatch_for_editing_with_name.groupby('TOKEN')['MODE'].transform(lambda x: (x == 0).sum())
    mismatch_for_editing_with_name['MODE_1_COUNT'] = mismatch_for_editing_with_name.groupby('TOKEN')['MODE'].transform(lambda x: (x == 1).sum())

    def determine_remarks_new(row):
        if row['MODE_0_COUNT'] == row['MODE_1_COUNT']:
            first_record = mismatch_for_editing_with_name[mismatch_for_editing_with_name['TOKEN'] == row['TOKEN']].iloc[0]
            if first_record['MODE'] == 1:
                return "day one out"
        return ""

    mismatch_for_editing_with_name['REMARKS'] = mismatch_for_editing_with_name.apply(determine_remarks_new, axis=1)

    mismatch_for_editing_with_name = mismatch_for_editing_with_name.drop(columns={'MODE'})
    mismatch_for_editing_with_name = mismatch_for_editing_with_name.drop_duplicates()

    mismatch['REMARKS'].replace('', pd.NA, inplace=True)

    # Filter the mismatch_check_results dataframe for rows that have remarks
    remarked_results = mismatch[mismatch['REMARKS'].notnull()]


    # Now substitute the remark for the corresponding TOKEN in mismatch_report
    for _, row in remarked_results.iterrows():
        token = row['TOKEN']
        remark = row['REMARKS']
        
        # Substitute the remark in the mismatch_report dataframe
        mismatch_for_editing_with_name.loc[mismatch_for_editing_with_name['TOKEN'] == token, 'REMARKS'] = remark
    if len(mismatch_for_editing_with_name) !=0:
        mismatch_for_editing_with_name.to_csv(table_paths['mismatch_report_path'],index=False)


    pytotpun_df = pd.concat([passed_punches_df,mismatch_punches_df], ignore_index=True)
    pytotpun_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)

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

    