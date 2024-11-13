from dbfread import DBF
from dbf_handler import dbf_2_df
import os
import pandas as pd
import requests
import shutil
from dbf import Table,READ_WRITE
from datetime import datetime,timedelta
from py_paths import g_first_path

def file_paths(curr_path):
    ## common paths
    new_txt_path = './new.txt'

    ## normal execution
    root_folder = curr_path
    dated_dbf = root_folder + 'dated.dbf'
    muster_dbf = root_folder + 'muster.dbf'
    holmast_dbf = root_folder + 'holmast.dbf'
    punches_dbf = root_folder + 'punches.dbf'
    lvform_dbf = root_folder + 'lvform.dbf'
    exe = False
    gsel_date_path = root_folder + 'gseldate.txt'
    g_option_path = root_folder + 'g_option.txt'
    wdtest_path = root_folder + 'wdtest.csv'
    wdtest_server_path = root_folder + 'wdtest_server.csv'
    wdtest_client_path = root_folder + 'wdtest_client.csv'
    passed_csv_path = root_folder + 'passed.csv'
    orphaned_punches_path = root_folder + 'orphaned_punches.csv'
    out_of_range_punches_path = root_folder + 'out_of_range_punches.csv'
    holmast_csv_path = root_folder + 'holiday.csv'
    lvform_csv_path = root_folder + 'leave.csv'
    pytotpun_dbf_path = root_folder + 'pytotpun.dbf'
    mismatch_report_path = root_folder + 'mismatch_report.csv'
    passed_punches_df_path = root_folder + 'passed_punches.csv'
    mismatch_punches_df_path = root_folder + 'mismatch_punches.csv'
    total_punches_punches_df_path = root_folder + 'total_punches_punches.csv'
    total_pytotpun_punches_df_path = root_folder + 'total_pytotpun_punches.csv'
    actual_punches_df_path = root_folder + 'actual_punches.csv'
    duplicate_punches_df_path = root_folder + 'duplicates_punches.csv'

    empty_tables_path = root_folder + 'empty_tables.txt'
    mismatch_csv_path = root_folder + 'mismatch.csv'

    muster_csv_path = root_folder + 'muster.csv'

    punch_csv_path = root_folder + 'punch.csv'
    final_csv_path = root_folder + 'final.csv'

    payroll_input_path = root_folder + 'payroll_input.csv'

    muster_role_path = root_folder + 'muster_role.csv'

    gsel_date_excluded_punches_len_df_path = root_folder + 'gseldate_punches.csv'

    dayone_out_path = root_folder + 'dayone_out_punches.csv'

    temp_gseldate_path = root_folder + 'temp_gsledate_pytotpun.csv'

    next_month_day_one_path = root_folder + 'next_month_day_one_final.csv'

    pundel_true_path = root_folder + 'pundel_true_punches.csv'

    ## exe
    # root_folder = curr_path
    # dated_dbf = './dated.dbf'
    # muster_dbf = './muster.dbf'
    # holmast_dbf = './holmast.dbf'
    # punches_dbf = './punches.dbf'
    # lvform_dbf = './lvform.dbf'
    # exe = True
    # gsel_date_path = './gseldate.txt'
    # g_option_path = './g_option.txt'
    # wdtest_path = './wdtest.csv'
    # wdtest_server_path = './wdtest_server.csv'
    # wdtest_client_path = './wdtest_client.csv'
    # passed_csv_path = './passed.csv'
    # orphaned_punches_path = './orphaned_punches.csv'
    # out_of_range_punches_path = './out_of_range_punches.csv'
    # holmast_csv_path = './holiday.csv'
    # lvform_csv_path = './leave.csv'
    # pytotpun_dbf_path = './pytotpun.dbf'
    # mismatch_report_path = './mismatch_report.csv'
    # passed_punches_df_path = './passed_punches.csv'
    # mismatch_punches_df_path = './mismatch_punches.csv'
    # total_punches_punches_df_path = './total_punches_punches.csv'
    # total_pytotpun_punches_df_path = './total_pytotpun_punches.csv'
    # actual_punches_df_path = './actual_punches.csv'
    # duplicate_punches_df_path = './duplicates_punches.csv'

    # empty_tables_path = './empty_tables.txt'
    # mismatch_csv_path = './mismatch.csv'

    # muster_csv_path = './muster.csv'

    # punch_csv_path = './punch.csv'
    # final_csv_path = './final.csv'

    # payroll_input_path = './payroll_input.csv'

    # muster_role_path = './muster_role.csv'

    # gsel_date_excluded_punches_len_df_path = './gseldate_punches.csv'

    # dayone_out_path = './dayone_out_punches.csv'

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
            "gsel_date_excluded_punches_len_df_path":gsel_date_excluded_punches_len_df_path,
            "dayone_out_path":dayone_out_path,
            "temp_gseldate_path":temp_gseldate_path,
            
            "next_month_day_one_path":next_month_day_one_path,
            "pundel_true_path":pundel_true_path}

def check_ankura(g_current_path):
    table_paths = file_paths(g_current_path)
    print("Exe status: ", table_paths['exe'])
    if table_paths['exe'] == True:
        with open(table_paths['new_txt_path']) as f:
            if f.read() == "Ankura@60":
                pass
        os.remove(table_paths['new_txt_path'])

def check_database():
    with open(g_first_path + "g_option.txt") as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        g_process_mode,g_pgdata = int(file_contents[1]), int(file_contents[0])
    return g_pgdata, g_process_mode, file_contents[-1]

def test_db_len(g_current_path):
    table_paths = file_paths(g_current_path)
    print(table_paths['dated_dbf_path'])
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

def punch_mismatch(g_current_path):
    gseldate_flag_file_exists = False
    gseldate_flag_date_range = False
    gseldate_flag_saved_date_lesser = False
    gseldate_flag_saved_and_curr_gseldate_equality = False
    saved_gseldate_exists = False

    table_paths = file_paths(g_current_path)
    dated_dbf = table_paths['dated_dbf_path']
    dated_table = DBF(dated_dbf, load=True) 
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    print("*************** start date: *************", start_date)
    print("*************** end date: *************", end_date)

    muster_dbf = table_paths['muster_dbf_path']
    muster_table = DBF(muster_dbf, load=True)
    muster_df = pd.DataFrame(iter(muster_table))
    print("mismatch muster_df ", muster_df['TOKEN'])

    punches_dbf = table_paths['punches_dbf_path']
    punches_table = DBF(punches_dbf, load=True)
    print("punches dbf length: ",len(punches_table))
    punches_df = pd.DataFrame(iter(punches_table))

    punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%d-%b-%y %H:%M:%S').dt.round('S')
    punches_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
    # punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    print(f"Before dropping duplicates: {punches_df.shape[0]}")

    with open(table_paths['gsel_date_path']) as file:
        file_contents = file.readlines()
        file_contents = [string.strip('\n') for string in file_contents]
        gseldate = file_contents[0]
        gsel_datetime = pd.to_datetime(gseldate)
        print(gseldate, type(gseldate))

    pytotpun_dbf = table_paths['pytotpun_dbf_path']
    pytotpun_table = DBF(pytotpun_dbf, load=True)
    pytotpun_df = pd.DataFrame(iter(pytotpun_table))
    # print("$$$$$$$$$$$$$$$$$$ pytotpun columns",pytotpun_df.columns, pytotpun_df['del'].isna().any())
    print("$$$$$$$$$$$$$$$$$$ pytotpun columns",pytotpun_df.columns)
    pytotpun_df.to_csv('pytotpun_before_modifying.csv',index=False)
    pytotpun_num_records = len(pytotpun_df)
    pundel_true_punches_columns = ["TOKEN", "COMCODE", "PDATE", "HOURS", "MINUTES", "MODE", "PDTIME", "MCIP", "DEL"]
    pundel_true_punches_df = pd.DataFrame(columns=pundel_true_punches_columns)
    if pytotpun_num_records !=0:
        print('********* Making pymismatch as punches **********')
        pytotpun_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
        # Print initial dtype
        print("Initial DEL column type:", pytotpun_df['DEL'].dtype)

        # Convert DEL to string type and handle None values
        
        pytotpun_df['DEL'] = pytotpun_df['DEL'].astype(str).replace("None", "").replace("", "False").fillna("False")

        pundel_true_punches_df = pytotpun_df[pytotpun_df['DEL'] == "True"]
        pundel_true_punches_df.to_csv(table_paths['pundel_true_path'],index=False)

        pytotpun_df = pytotpun_df[pytotpun_df['DEL'] != "True"]

        # Print final dtype to confirm
        print("Updated DEL column type:", pytotpun_df['DEL'].dtype)
        pytotpun_df.to_csv('just_to_check_pytotpun.csv',index=False)
        # pytotpun_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        punches_df = pytotpun_df
        pytotpun_df.to_csv(table_paths['total_pytotpun_punches_df_path'],index=False)
        if os.path.exists(table_paths['gsel_date_excluded_punches_len_df_path']):
            gseldate_flag_file_exists = True
            print("gsel date file exists: ",gseldate_flag_file_exists)
            saved_gseldate_data = pd.read_csv(table_paths['gsel_date_excluded_punches_len_df_path'],dtype={'COMCODE': str})
            print('saved gseldate: ',saved_gseldate_data)
            print("len of saved gseldate",len(saved_gseldate_data))
            if len(saved_gseldate_data) !=0:

                # saved_gseldate_data['PDTIME'] = saved_gseldate_data['PDATE'].dt.date

                print('saved gseldate', saved_gseldate_data)

                print('saved gseldate: ',saved_gseldate_data['PDATE'].iloc[0], type(saved_gseldate_data['PDATE'].iloc[0]))
                print('gseldate: ',gseldate, type(gseldate))

                if saved_gseldate_data['PDATE'].iloc[0] <= gseldate:
                    gseldate_flag_saved_date_lesser = True
                    print('saved gseldate is lesser than gseldate', gseldate_flag_saved_date_lesser)

                    pytotpun_df['PDTIME'] = pd.to_datetime(pytotpun_df['PDTIME'])
                    # pytotpun_df['PDATE'] = pytotpun_df['PDATE'].dt.date

                    pytotpun_keys = set(zip(pytotpun_df['TOKEN'], pytotpun_df['PDATE']))

                    saved_gseldate_data['in_pytotpun'] = saved_gseldate_data.apply(
                        lambda row: (row['TOKEN'], row['PDATE']) in pytotpun_keys, axis=1
                    )

                    rows_to_move = saved_gseldate_data[~saved_gseldate_data['in_pytotpun']].drop(columns=['in_pytotpun'])

                    pytotpun_df_new = pd.concat([pytotpun_df, rows_to_move], ignore_index=True)
                    pytotpun_df_new.sort_values(by=['TOKEN', 'PDATE'], inplace=True)
                    # pytotpun_df_new.sort_values(by=['TOKEN', 'PDATE', 'MODE'], inplace=True)
                    # pytotpun_df_new.to_csv(table_paths['temp_gseldate_path'],index=False)

                    saved_gseldate_data = saved_gseldate_data[saved_gseldate_data['in_pytotpun']].drop(columns=['in_pytotpun'])
                    print("saved gseldate data",len(saved_gseldate_data))
                    if len(saved_gseldate_data)!=0:

                        saved_gseldate_exists = True
                        saved_gseldate_data_date_format = datetime.strptime(saved_gseldate_data['PDATE'].iloc[0], "%Y-%m-%d").date()
                        print(saved_gseldate_data_date_format)
                        saved_gseldate_data.to_csv(table_paths['gsel_date_excluded_punches_len_df_path'],index=False)

                    pytotpun_df_new['PDATE'] = pd.to_datetime(pytotpun_df_new['PDATE'])
                    pytotpun_df_new['PDATE'] = pytotpun_df_new['PDATE'].dt.date

                    punches_df = pytotpun_df_new
                    punches_df.to_csv('del_check_punches.csv',index=False)

                    print("******************* punches len *****************", len(punches_df))

                    # elif saved_gseldate_data['PDATE'].iloc[0] == gsel_datetime:
                    #     gseldate_flag_saved_and_curr_gseldate_equality = True
                    #     print('gseldate_flag_saved_and_curr_gseldate_equality:', gseldate_flag_saved_and_curr_gseldate_equality)

                if saved_gseldate_exists == True:

                    if start_date <= saved_gseldate_data_date_format <= end_date:
                        gseldate_flag_date_range = True
                        print("gsel date date range: ",gseldate_flag_date_range)
                        print(f"The date falls within the range.")
                    else:
                        print(f"The date does not fall within the range.")
                        if os.path.exists(table_paths['gsel_date_excluded_punches_len_df_path']):
                            os.remove(table_paths['gsel_date_excluded_punches_len_df_path'])
                            print(f"{table_paths['gsel_date_excluded_punches_len_df_path']} has been deleted.")

        punches_df['PDTIME'] = pd.to_datetime(punches_df['PDTIME'], format='%Y-%m-%d %H:%M:%S').dt.round('S')
        
        # pytotpun_df.to_csv(table_paths['total_pytotpun_punches_df_path'],index=False)
    elif pytotpun_num_records == 0:
        if os.path.exists(table_paths['gsel_date_excluded_punches_len_df_path']):
            os.remove(table_paths['gsel_date_excluded_punches_len_df_path'])
            print(f"{table_paths['gsel_date_excluded_punches_len_df_path']} has been deleted.")
        else:
            print(f"{table_paths['gsel_date_excluded_punches_len_df_path']} does not exist.")

        # punches_df['DEL'].fillna(False, inplace=True)
        # punches_df['DEL'] = False

        if 'DEL' in pytotpun_df.columns:
            print("Column exists")
            pytotpun_df['DEL'].fillna(False, inplace=True)
            pytotpun_df['DEL'] = pytotpun_df['DEL'].astype(bool)
        else:
            print("Column does not exist")

        # if 'DEL' not in pytotpun_df.columns:
        #     # Create the column 'DEL' and initialize with False
        #     pytotpun_df['DEL'] = False
        # else:
        #     # Fill NaN values with False in existing 'DEL' column
        #     pytotpun_df['DEL'].fillna(False, inplace=True)

        # # Ensure 'DEL' column is of boolean type
        # pytotpun_df['DEL'] = pytotpun_df['DEL'].astype(bool)

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

    out_of_range_punches_df = unique_punches_df[
        ~(
            (unique_punches_df['PDATE'] >= start_date) &
            (unique_punches_df['PDATE'] <= end_date)
        ) | 
        (unique_punches_df['PDATE'] > gsel_datetime)
    ]
    out_of_range_punches_df.to_csv(table_paths['out_of_range_punches_path'],index=False)

    merged_df = unique_punches_df.merge(out_of_range_punches_df, on=['TOKEN','PDTIME','MODE'], how='inner')
    punches_df = unique_punches_df[~unique_punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    print(f"After removing out of range punches: {punches_df.shape[0]}")

    merged_orphaned_df = punches_df.merge(muster_df, on='TOKEN', how='outer', indicator=True)
    orphaned_df = merged_orphaned_df[merged_orphaned_df['_merge'] == 'left_only']
    orphaned_df = orphaned_df.drop(columns=['_merge'])
    print("Orphaned columns: ",orphaned_df.columns)
    if pytotpun_num_records == 0:
        orphaned_punches_df = orphaned_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP']]
        orphaned_punches_df = orphaned_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
        orphaned_punches_df['DEL'] = "False"
        orphaned_punches_df.to_csv(table_paths['orphaned_punches_path'],index=False)
    else:
        orphaned_punches_df = orphaned_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','DEL_x']]
        orphaned_punches_df = orphaned_punches_df.rename(columns={'COMCODE_y':'COMCODE','DEL_x':'DEL'})
        orphaned_punches_df.to_csv(table_paths['orphaned_punches_path'],index=False)
        
    merged_df = punches_df.merge(orphaned_punches_df, on=['TOKEN', 'PDTIME','MODE'], how='inner')
    punches_df = punches_df[~punches_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index.isin(merged_df.set_index(['TOKEN', 'PDTIME', 'MODE']).index)]
    print(f"After removing orphaned punches: {punches_df.shape[0]}")
    punches_df.to_csv(table_paths['actual_punches_df_path'],index=False)

    dayone_out = pd.DataFrame(columns=punches_df.columns)

    # Iterate through the grouped tokens
    for token, token_group in punches_df.groupby('TOKEN'):
        # Ensure PDATE is in datetime format
        token_group['PDATE'] = pd.to_datetime(token_group['PDATE'])
        
        ### First Logic: Check if the first day of the month has MODE == 1 ###
        # Filter for the first day of the month
        first_day_of_month = token_group[token_group['PDATE'].dt.day == 1]
        
        if not first_day_of_month.empty:
            # Sort rows from the 1st day by PDTIME
            first_day_rows = first_day_of_month.sort_values(by='PDTIME')
            
            # Filter for rows where MODE == 1
            mode_1_rows = first_day_rows[first_day_rows['MODE'] == 1]
            
            if len(mode_1_rows) > 1:
                # Select the first MODE == 1 row
                first_mode_1_row = mode_1_rows.iloc[0]
                
                # Append this row to dayone_out
                dayone_out = pd.concat([dayone_out, first_day_rows[first_day_rows.index == first_mode_1_row.name]])
                
                # Remove the first MODE == 1 row from punches_df, only if it exists
                if first_mode_1_row.name in punches_df.index:
                    punches_df = punches_df.drop(index=first_mode_1_row.name)
                else:
                    print(f"Index {first_mode_1_row.name} not found in punches_df during first logic.")

        ### Second Logic: Check if the first row for the token has MODE == 1 ###
        # Sort the token group by PDATE and PDTIME to ensure we get the first chronological row
        token_group = token_group.sort_values(by=['PDATE', 'PDTIME'])
        
        # Check if the first row in the token group has MODE == 1
        if token_group.iloc[0]['MODE'] == 1:
            # Select the first row with MODE == 1 (which is already the first row in this case)
            first_mode_1_row = token_group.iloc[0]
            
            # Append this row to dayone_out if it hasn't already been added
            if first_mode_1_row.name not in dayone_out.index:
                dayone_out = pd.concat([dayone_out, token_group[token_group.index == first_mode_1_row.name]])
            
            # Remove the first MODE == 1 row from punches_df, only if it exists
            if first_mode_1_row.name in punches_df.index:
                punches_df = punches_df.drop(index=first_mode_1_row.name)
            else:
                print(f"Index {first_mode_1_row.name} not found in punches_df during second logic.")

    dayone_out.to_csv(table_paths['dayone_out_path'], index=False)

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

    mismatch_status = True

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

    # punches_df.to_csv('just_to_check.csv',index=False)

    for _, group in punches_df.groupby(['TOKEN']):
        # Check if the pattern is correct
        if group.iloc[0]['MODE'] == 0 and check_pattern(group):
            passed = pd.concat([passed, group])
        else:
            # Check if the last mode is 0 and if it matches the specific date
            if group.iloc[-1]['MODE'] == 0:
                if group.iloc[-1]['PDTIME'].date() == gsel_datetime.date():
                    # Move the last row to gseldate_punches
                    gseldate_punches = pd.concat([gseldate_punches, group.iloc[[-1]]])
                    
                    # Recheck only if more than one row remains
                    if len(group) > 1:
                        remaining_group = group.iloc[:-1]
                        if check_pattern(remaining_group):
                            passed = pd.concat([passed, remaining_group])
                        else:
                            mismatch = pd.concat([mismatch, remaining_group])
                    else:
                        passed = pd.concat([passed, group.iloc[:-1]])
                else:
                    mismatch = pd.concat([mismatch, group])
            else:
                mismatch = pd.concat([mismatch, group])

    if len(gseldate_punches) !=0:
        # Save the gseldate_punches to CSV
        gseldate_punches['DEL'] = "False"
        gseldate_punches.to_csv(table_paths['gsel_date_excluded_punches_len_df_path'], index=False)

    # Add a new column 'Remarks'
    mismatch['REMARKS'] = ""

    def check_pattern_stop_on_first_break_v6(df):
        mode_counts = {0: 0, 1: 0}
        pattern_broken = False
        unmatched_zero_row = None  # Track if we encounter an unmatched mode=0 and its row index
        
        # If there is only one row and mode=0, it's automatically a pattern break
        if len(df) == 1 and df.iloc[0]['MODE'] == 0:
            df.iloc[0, df.columns.get_loc('REMARKS')] = df.iloc[0]['PDTIME']
            return df

        for i in range(1, len(df)):
            current_mode = df.iloc[i]['MODE']
            previous_mode = df.iloc[i-1]['MODE']
            
            # Update the mode counts
            mode_counts[current_mode] += 1
            
            # Check if there is a mode=0 without a matching mode=1
            if current_mode == 1:
                mode_counts[0] = 0  # Reset the count when mode=1 is encountered
                unmatched_zero_row = None  # Reset unmatched_zero_row because mode=1 is found
            elif current_mode == 0:
                unmatched_zero_row = i  # Record the index of the unmatched mode=0
            
            # If mode=0 doesn't have a corresponding mode=1
            if unmatched_zero_row is not None and mode_counts[1] == 0 and not pattern_broken:
                df.iloc[unmatched_zero_row, df.columns.get_loc('REMARKS')] = df.iloc[unmatched_zero_row]['PDTIME']
                pattern_broken = True  # Stop further checks after the first break
                break
            
            # Check if the pattern is broken (two consecutive equal modes)
            if current_mode == previous_mode and not pattern_broken:
                df.iloc[i, df.columns.get_loc('REMARKS')] = df.iloc[i]['PDTIME']
                pattern_broken = True  # Stop further checks after the first break
                break
        
        # Final check if there's any unmatched mode=0 without a corresponding mode=1 at the end
        if not pattern_broken and unmatched_zero_row is not None:
            df.iloc[unmatched_zero_row, df.columns.get_loc('REMARKS')] = df.iloc[unmatched_zero_row]['PDTIME']
        
        return df

    # Apply the pattern check for each TOKEN
    mismatch = mismatch.groupby('TOKEN', group_keys=False).apply(check_pattern_stop_on_first_break_v6)


    if len(mismatch) ==0:

        columns = ['TOKEN','COMCODE','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','REMARKS']
        mismatch = pd.DataFrame(columns=columns)

    result_passed_df = pd.merge(passed, muster_df, on='TOKEN', how='inner')
    print("result passed df cols: ",result_passed_df.columns)

    if pytotpun_num_records == 0:
        passed_punches_df = result_passed_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','DEL']]
        passed_punches_df = passed_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
        passed_punches_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
        passed_punches_df['DEL'] = "False"
        # passed_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        passed_punches_df.to_csv(table_paths['passed_punches_df_path'],index=False)
    else:
        passed_punches_df = result_passed_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','DEL_x']]
        passed_punches_df = passed_punches_df.rename(columns={'COMCODE_y':'COMCODE','DEL_x':'DEL'})
        passed_punches_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
        # passed_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        passed_punches_df.to_csv(table_paths['passed_punches_df_path'],index=False)
        
    mismatch['PDTIME'] = pd.to_datetime(mismatch['PDTIME'])

    if ((mismatch['MODE'] == 0) & (mismatch['PDTIME'].dt.date == gsel_datetime.date())).any():
        mismatch_status = False

    result_mismatch_df = pd.merge(mismatch, muster_df, on='TOKEN', how='inner')
    print("Mismatch punches columns: ",result_mismatch_df.columns)


    if pytotpun_num_records == 0:
        mismatch_punches_df = result_mismatch_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','DEL']]
        mismatch_punches_df = mismatch_punches_df.rename(columns={'COMCODE_y':'COMCODE'})
        mismatch_punches_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
        mismatch_punches_df['DEL'] = "False"
        # mismatch_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        mismatch_punches_df.to_csv(table_paths['mismatch_punches_df_path'],index=False)
    else:
        mismatch_punches_df = result_mismatch_df[['TOKEN','COMCODE_y','PDATE','HOURS','MINUTES','MODE','PDTIME','MCIP','DEL_x']]
        mismatch_punches_df = mismatch_punches_df.rename(columns={'COMCODE_y':'COMCODE','DEL_x':'DEL'})
        mismatch_punches_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
        # mismatch_punches_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
        mismatch_punches_df.to_csv(table_paths['mismatch_punches_df_path'],index=False)

    if len(mismatch) !=0:
        mismatch_status = True
        mismatch_for_editing_merged_with_muster = pd.merge(mismatch, muster_df, on='TOKEN', how='inner')
        mismatch_for_editing_with_name = mismatch_for_editing_merged_with_muster[['TOKEN','NAME','EMPCODE','MODE']]
        mismatch_for_editing_with_name = mismatch_for_editing_with_name.rename(columns={'COMCODE_y':'COMCODE'})

        mismatch_for_editing_with_name['MODE_0_COUNT'] = mismatch_for_editing_with_name.groupby('TOKEN')['MODE'].transform(lambda x: (x == 0).sum())
        mismatch_for_editing_with_name['MODE_1_COUNT'] = mismatch_for_editing_with_name.groupby('TOKEN')['MODE'].transform(lambda x: (x == 1).sum())

        def determine_remarks_new(row):
            # if row['MODE_0_COUNT'] == row['MODE_1_COUNT']:
            first_record = mismatch_for_editing_with_name[mismatch_for_editing_with_name['TOKEN'] == row['TOKEN']].iloc[0]
            if first_record['MODE'] == 1:
                return "day one out"
            # return ""

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

    gseldate_date_format = datetime.strptime(gseldate, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    if is_last_day == True:

        # Load the DBF file into a DataFrame
        punches_dbf_new_month = table_paths['punches_dbf_path']  
        punches_table_new_month = DBF(punches_dbf_new_month, load=True)
        print("punches dbf length: ", len(punches_table_new_month))
        punches_df_new_month = pd.DataFrame(iter(punches_table_new_month))
        punches_df_new_month['DEL'] = False
        print("punches df new month columns: ",punches_df_new_month.columns)

        # Convert 'end_date' to datetime and add one day
        end_date_dt = pd.to_datetime(end_date)
        end_date_plus1 = end_date_dt + pd.Timedelta(days=1)

        # Convert the dates to date format for filtering
        end_date_date = end_date_dt.date()
        end_date_plus1_date = end_date_plus1.date()

        # Ensure 'PDATE' column is in date format
        punches_df_new_month['PDATE'] = pd.to_datetime(punches_df_new_month['PDATE']).dt.date

        # Filter the DataFrame by 'PDATE'
        filtered_df = punches_df_new_month[
            punches_df_new_month['PDATE'].between(end_date_date, end_date_plus1_date)
        ]

        # Sort by TOKEN, PDATE, and PDTIME to ensure the order of events is maintained
        filtered_df.sort_values(by=['TOKEN', 'PDATE', 'PDTIME'], inplace=True)

        # Initialize an empty list to store matching records
        matching_records = []

        # Iterate through each TOKEN group to find consecutive MODE=0 (end_date) and MODE=1 (end_date_plus1_date)
        for token, group in filtered_df.groupby('TOKEN'):
            group = group.reset_index(drop=True)  # Reset index for easier iteration
            for i in range(len(group) - 1):
                if (
                    group.loc[i, 'PDATE'] == end_date_date
                    and group.loc[i, 'MODE'] == 0
                    and group.loc[i + 1, 'PDATE'] == end_date_plus1_date
                    and group.loc[i + 1, 'MODE'] == 1
                ):
                    # Add the matching rows to the list
                    matching_records.append(group.loc[i])
                    matching_records.append(group.loc[i + 1])

        # Convert the matching records to a DataFrame
        consecutive_matching_df = pd.DataFrame(matching_records)

        # Save the full matching records to a CSV
        consecutive_matching_df.to_csv('consecutive_matching_records.csv', index=False)

        print("Filtered consecutive matching records saved to 'consecutive_matching_records.csv'")

        # === Additional Step: Filter Out Only MODE=1 Records ===
        if not consecutive_matching_df.empty:  # Check if DataFrame has rows
            mode_1_only_df = consecutive_matching_df[consecutive_matching_df['MODE'] == 1]
            mode_1_only_df['DEL'] = "False"
        else:
            # Create an empty DataFrame with the desired columns
            mode_1_only_df = pd.DataFrame(columns=[
                'TOKEN', 'COMCODE', 'PDATE', 'HOURS', 'MINUTES', 
                'MODE', 'PDTIME', 'MCIP', 'DEL'
            ])

        print("Filtered MODE=1 records saved to 'mode_1_only_records.csv'")
    

        # day_one_next_month = filtered_df[filtered_df["MODE"] == 1]
        # day_one_next_month.to_csv('day_one_next_month.csv',index=False)

        # df_first_occurrence = day_one_next_month.drop_duplicates(subset=["TOKEN"], keep="first")
        # df_first_occurrence.to_csv('df_first_occurance.csv',index=False)
        

        # matching_tokens = gseldate_punches[gseldate_punches["MODE"] == 0]["TOKEN"]
        # next_month_day_one_final = df_first_occurrence[(df_first_occurrence["TOKEN"].isin(matching_tokens)) & (df_first_occurrence["MODE"] == 1)]

        # next_month_day_one_final.to_csv(table_paths['next_month_day_one_path'],index=False)

        pytotpun_df = pd.concat([passed_punches_df,mismatch_punches_df,gseldate_punches,mode_1_only_df,pundel_true_punches_df], ignore_index=True)
    else:
        pytotpun_df = pd.concat([passed_punches_df,mismatch_punches_df], ignore_index=True)
    # pytotpun_df.sort_values(by=['TOKEN', 'PDTIME', 'MODE'], inplace=True)
    pytotpun_df.sort_values(by=['TOKEN', 'PDTIME'], inplace=True)
    # pytotpun_df['DEL'] = pytotpun_df['DEL'].replace("", "False")
    pytotpun_df['DEL'] = pytotpun_df['DEL'].map({"False": False, "True": True})
    pytotpun_df.to_csv('last_check_of_pytotpun.csv',index=False)
    # Check if 'DEL' is in the DataFrame columns
    # if 'DEL' in pytotpun_df.columns:
    # #     # Replace empty strings with NaN so they can be filled with False
    # #     pytotpun_df['DEL'].replace('', pd.NA, inplace=True)
    #     # Now convert the column to boolean, where NaN will become False
    #     pytotpun_df['DEL'] = pytotpun_df['DEL'].astype(bool)

    pytotpun_df.to_csv(table_paths['total_pytotpun_punches_df_path'],index=False)

    pytotpun_df['PDATE'] = pd.to_datetime(pytotpun_df['PDATE'])
    pytotpun_df['PDATE'] = pytotpun_df['PDATE'].dt.date

    table = Table(table_paths['pytotpun_dbf_path'])
    table.open(mode=READ_WRITE)
    table.zap()

    for index, row in pytotpun_df.iterrows():
        record = {field: row[field] for field in table.field_names if field in pytotpun_df.columns}
        table.append(record)
    table.close()

    pytotpun_df.to_csv('last_check_after_modify_of_pytotpun.csv',index=False)

    passed_df_len = result_passed_df.shape[0]
    print("passed csv len: ",passed_df_len)
    mismatch_df_len = result_mismatch_df.shape[0]
    print("mismatch df len: ",mismatch_df_len)

    if mismatch_status == True:
        return 1,result_mismatch_df,punches_df
    else:
        return 1,None
    
def server_collect_db_data(g_first_path):
    table_paths = file_paths(g_first_path)
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

def client_collect_db_data(g_first_path):
    table_paths = file_paths(g_first_path)
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

def create_wdtest(server_df,client_df,g_first_path):
    table_paths = file_paths(g_first_path)
    result_df = pd.merge(server_df, client_df, on=['TOKEN', 'PDATE', 'PDTIME', 'MCIP'], how='left', indicator=True)
    print("server", server_df.dtypes)
    print("client", client_df.dtypes)

    result_df = result_df[result_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    print("wdtest", os.getcwd())
    
    result_df.to_csv(table_paths['wdtest_path'],index=False)

    