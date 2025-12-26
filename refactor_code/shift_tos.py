from punch import generate_punch
from muster import generate_muster
from test import (
    test_db_len, make_blank_files, delete_old_files, create_new_csvs,
    punch_mismatch, file_paths, check_ankura, check_database,
    server_collect_db_data, client_collect_db_data, create_wdtest, check_g_main_path
)
from payroll_input import pay_input
import pandas as pd
import sys
import os
from dbf_handler import dbf_2_df
from py_paths import g_current_path, g_first_path


def create_final_csv(muster_df, punch_df, mismatch_df, g_current_path, mode_1_only_df):
    # ---------------- Ensure COMCODE exists before merge ----------------
    if "COMCODE" not in muster_df.columns and "COMCODE" not in punch_df.columns:
        muster_df = muster_df.copy()
        punch_df = punch_df.copy()
        muster_df["COMCODE"] = ""
        punch_df["COMCODE"] = ""

    # ---------------- Ensure TOTPASSHRS exists ONLY FROM PUNCH ----------------
    # Requirement: TOTPASSHRS must come from punch_df itself.
    # So we keep it from punch_df, and if missing there, create blank there.
    if "TOTPASSHRS" not in punch_df.columns:
        punch_df = punch_df.copy()
        punch_df["TOTPASSHRS"] = ""

    # Standardize types
    if "COMCODE" in muster_df.columns:
        muster_df = muster_df.copy()
        muster_df["COMCODE"] = muster_df["COMCODE"].astype(str).str.strip()
    if "COMCODE" in punch_df.columns:
        punch_df = punch_df.copy()
        punch_df["COMCODE"] = punch_df["COMCODE"].astype(str).str.strip()

    # Keep TOTPASSHRS as string (safe for CSV)
    punch_df["TOTPASSHRS"] = punch_df["TOTPASSHRS"].fillna("").astype(str).str.strip()

    punch_df["PDATE"] = pd.to_datetime(punch_df["PDATE"])
    muster_df["PDATE"] = pd.to_datetime(muster_df["PDATE"])

    # ---------------- Merge ----------------
    # We do NOT want TOTPASSHRS from muster at all.
    # If muster_df contains TOTPASSHRS for any reason, drop it before merge.
    if "TOTPASSHRS" in muster_df.columns:
        muster_df = muster_df.drop(columns=["TOTPASSHRS"], errors="ignore")

    merged_df = pd.merge(
        muster_df,
        punch_df,
        on=["TOKEN", "PDATE"],
        how="outer",
        suffixes=("_M", "_P")
    )

    # ---------------- Normalize COMCODE ----------------
    if "COMCODE" not in merged_df.columns:
        if "COMCODE_M" in merged_df.columns and "COMCODE_P" in merged_df.columns:
            merged_df["COMCODE"] = merged_df["COMCODE_M"].fillna("").astype(str).str.strip()
            mask_blank = merged_df["COMCODE"].eq("")
            merged_df.loc[mask_blank, "COMCODE"] = merged_df.loc[mask_blank, "COMCODE_P"].fillna("").astype(str).str.strip()
            merged_df.drop(columns=["COMCODE_M", "COMCODE_P"], inplace=True)
        elif "COMCODE_M" in merged_df.columns:
            merged_df["COMCODE"] = merged_df["COMCODE_M"]
            merged_df.drop(columns=["COMCODE_M"], inplace=True)
        elif "COMCODE_P" in merged_df.columns:
            merged_df["COMCODE"] = merged_df["COMCODE_P"]
            merged_df.drop(columns=["COMCODE_P"], inplace=True)
        else:
            merged_df["COMCODE"] = ""

    # ---------------- Normalize TOTPASSHRS (PUNCH ONLY) ----------------
    # After merge, it may appear as:
    # - TOTPASSHRS (if no conflict)
    # - TOTPASSHRS_P (if some other df had it)
    if "TOTPASSHRS" not in merged_df.columns:
        if "TOTPASSHRS_P" in merged_df.columns:
            merged_df["TOTPASSHRS"] = merged_df["TOTPASSHRS_P"].fillna("").astype(str).str.strip()
            merged_df.drop(columns=["TOTPASSHRS_P"], inplace=True)
        else:
            merged_df["TOTPASSHRS"] = ""
    else:
        merged_df["TOTPASSHRS"] = merged_df["TOTPASSHRS"].fillna("").astype(str).str.strip()

    # ---------------- Your existing logic ----------------
    mask = merged_df["MUSTER_STATUS"] == ""
    merged_df.loc[mask, "MUSTER_STATUS"] = merged_df.loc[mask, "PUNCH_STATUS"]
    merged_df = merged_df.rename(columns={"MUSTER_STATUS": "STATUS"})

    table_paths = file_paths(g_current_path)

    with open(table_paths["gsel_date_path"]) as file:
        file_contents = [string.strip("\n") for string in file.readlines()]
        gseldate = pd.to_datetime(file_contents[0])

    if "STATUS" in merged_df.columns:
        combined_condition = (
            ((merged_df["PDATE"] < merged_df["DATE_JOIN"]) |
             (merged_df["PDATE"] > merged_df["DATE_LEAVE"])) |
            (merged_df["PDATE"] > gseldate)
        )
        merged_df.loc[combined_condition, "STATUS"] = "--"
    else:
        print("'STATUS' column does not exist in the DataFrame.")

    # IMPORTANT: do NOT drop COMCODE or TOTPASSHRS
    drop_cols = [
        "DATE_JOIN", "DATE_LEAVE", "PUNCH_STATUS",
        "INTIME1", "OUTTIME1", "INTIME2", "OUTTIME2",
        "INTIME3", "OUTTIME3", "INTIME4", "OUTTIME4"
    ]
    merged_df = merged_df.drop([c for c in drop_cols if c in merged_df.columns], axis=1, errors="ignore")

    merged_df.loc[merged_df["STATUS"].isin(["WO", "PH"]), "OT"] = merged_df["TOTALTIME"]

    mismatch_report_df = pd.read_csv(table_paths["mismatch_report_path"])

    merged_df["TOKEN"] = merged_df["TOKEN"].astype(str)
    mismatch_report_df["TOKEN"] = mismatch_report_df["TOKEN"].astype(str)

    merged_df["PDATE"] = pd.to_datetime(merged_df["PDATE"], errors="coerce")
    mismatch_report_df["REMARKS"] = pd.to_datetime(mismatch_report_df["REMARKS"], errors="coerce")

    mismatch_report_df["cutoff_date"] = mismatch_report_df["REMARKS"].dt.date
    cutoff_map = mismatch_report_df.set_index("TOKEN")["cutoff_date"]
    merged_df["cutoff_date"] = merged_df["TOKEN"].map(cutoff_map)

    mask = merged_df["cutoff_date"].notna() & (merged_df["PDATE"].dt.date >= merged_df["cutoff_date"])
    merged_df.loc[mask, "STATUS"] = "MM"
    merged_df.drop(columns=["cutoff_date"], inplace=True)

    status_counts_by_empcode = merged_df.groupby(["TOKEN", "STATUS"])["STATUS"].count().unstack().reset_index()
    status_counts_by_empcode["HD"] = status_counts_by_empcode.get("HD", 0) / 2 if "HD" in status_counts_by_empcode else 0
    status_counts_by_empcode = status_counts_by_empcode.fillna(0)

    merged_df = pd.merge(merged_df, status_counts_by_empcode, on="TOKEN")

    merged_df["TOT_AB"] = merged_df.get("AB", 0)
    merged_df["TOT_WO"] = merged_df.get("WO", 0)
    merged_df["TOT_PR"] = (merged_df.get("PR", 0) + merged_df.get("HD", 0)).fillna(0)
    merged_df["TOT_PH"] = merged_df.get("PH", 0)
    merged_df["TOT_LV"] = merged_df.get("CL", 0) + merged_df.get("EL", 0) + merged_df.get("SL", 0)
    merged_df["TOT_MM"] = merged_df.get("MM", 0)

    merged_df = merged_df.drop_duplicates(subset=["TOKEN", "PDATE"])
    merged_df = merged_df.sort_values(by=["TOKEN", "PDATE"]).reset_index(drop=True)

    for i in range(1, len(merged_df) - 1):
        if (
            merged_df.at[i, "STATUS"] == "WO"
            and merged_df.at[i - 1, "STATUS"] == "AB"
            and merged_df.at[i + 1, "STATUS"] == "AB"
        ):
            total_time = merged_df.at[i, "TOTALTIME"]
            if pd.notna(total_time) and str(total_time).strip() != "":
                continue
            merged_df.at[i, "STATUS"] = "AB"

    merged_df["TOKEN"] = merged_df["TOKEN"].astype("int")

    columns_to_drop = ["HD", "AB", "PH", "PR", "WO", "CL", "EL", "SL", "--", "MM"]
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df], errors="ignore")

    merged_df = merged_df.drop(columns=["SHIFT_STATUS_y", "SHIFT_ST_y", "SHIFT_ED_y", "WORKHRS_y"], errors="ignore")
    merged_df.rename(columns={
        "SHIFT_STATUS_x": "SHIFT_STATUS",
        "SHIFT_ST_x": "SHIFT_ST",
        "SHIFT_ED_x": "SHIFT_ED",
        "WORKHRS_x": "WORKHRS"
    }, inplace=True)

    # ---------------- MM BLANKING (FINAL STEP) ----------------
    mm_mask = merged_df.get("STATUS", pd.Series(index=merged_df.index, dtype="object")) \
                       .astype(str).str.strip().str.upper().eq("MM")

    if "TOTALTIME" not in merged_df.columns:
        merged_df["TOTALTIME"] = ""
    if "OT" not in merged_df.columns:
        merged_df["OT"] = ""

    merged_df.loc[mm_mask, "TOTALTIME"] = ""
    merged_df.loc[mm_mask, "OT"] = ""

    # ---------------- INSERT BLANK COLUMN AFTER SHIFT_STATUS ----------------
    BLANK_COL_NAME = "BLANK_DATETIME"
    if BLANK_COL_NAME not in merged_df.columns:
        merged_df[BLANK_COL_NAME] = ""

    if "SHIFT_STATUS" in merged_df.columns:
        cols = list(merged_df.columns)
        cols.remove(BLANK_COL_NAME)
        insert_at = cols.index("SHIFT_STATUS") + 1
        cols.insert(insert_at, BLANK_COL_NAME)
        merged_df = merged_df[cols]

    desired_order = [
        "TOKEN", "COMCODE", "NAME", "EMPCODE", "EMP_DEPT", "DEPT_NAME", "EMP_DESI", "DESI_NAME",
        "PDATE", "STATUS", "INTIME", "OUTTIME", "TOTALTIME", "REMARKS", "OT",
        "TOT_AB", "TOT_WO", "TOT_PR", "TOT_PH", "TOT_LV", "TOT_MM",
        "SHIFT_STATUS", "BLANK_DATETIME", "SHIFT_ST", "SHIFT_ED", "WORKHRS",
        "inc_grt_minutes", "gratime_minutes", "workhrs_minutes", "halfday_minutes",
        "workhrs", "shift_st_time", "shift_ed_time", "TOTAL_HRS", "OD"
    ]

    existing_desired = [c for c in desired_order if c in merged_df.columns]
    remaining = [c for c in merged_df.columns if c not in existing_desired]
    merged_df = merged_df[existing_desired + remaining]

    # ---------------- FORCE TOTPASSHRS TO BE LAST COLUMN (FROM PUNCH ONLY) ----------------
    if "TOTPASSHRS" not in merged_df.columns:
        merged_df["TOTPASSHRS"] = ""
    cols = [c for c in merged_df.columns if c != "TOTPASSHRS"] + ["TOTPASSHRS"]
    merged_df = merged_df[cols]

    merged_df.to_csv(table_paths["final_csv_path"], index=False)

    # Final safety: pay_input expects COMCODE
    if "COMCODE" not in merged_df.columns:
        merged_df["COMCODE"] = ""

    pay_input(merged_df, g_current_path)



# try:
curr_root_folder = check_g_main_path()
g_current_path = curr_root_folder
# g_current_path = current_path
print("g current path: ",g_current_path)
pg_data_flag, process_mode_flag, current_path = check_database()
check_ankura(g_current_path)
print(pg_data_flag, type(pg_data_flag))
print(process_mode_flag, type(process_mode_flag))
table_paths = file_paths(g_current_path)
create_new_csvs(table_paths['muster_csv_path'],['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','DATE_JOIN','DATE_LEAVE','PDATE','MUSTER_STATUS'],
                table_paths['punch_csv_path'],['TOKEN','PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','PUNCH_STATUS'],
                table_paths['final_csv_path'],['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','PDATE','STATUS','INTIME','OUTTIME','TOTALTIME','REMARKS','TOT_AB','TOT_WO','TOT_PR','TOT_PH','TOT_LV'])
delete_old_files(table_paths['mismatch_csv_path'])
make_blank_files(table_paths['muster_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','DATE_JOIN','DATE_LEAVE','PDATE','MUSTER_STATUS'])
make_blank_files(table_paths['punch_csv_path'],columns=['TOKEN','PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','PUNCH_STATUS'])
make_blank_files(table_paths['final_csv_path'],columns=['TOKEN','COMCODE','NAME','EMPCODE','EMP_DEPT','DEPT_NAME','EMP_DESI','DESI_NAME','PDATE','STATUS','INTIME','OUTTIME','TOTALTIME','REMARKS','TOT_AB','TOT_WO','TOT_PR','TOT_PH','TOT_LV'])
make_blank_files(table_paths['empty_tables_path'])
delete_old_files(table_paths['mismatch_csv_path'])
delete_old_files(table_paths['payroll_input_path'])

delete_old_files(table_paths['passed_csv_path'])
# delete_old_files(table_paths['orphaned_punches_path'])

# delete_old_files(table_paths['out_of_range_punches_path'])

# delete_old_files(table_paths['passed_punches_df_path'])
# delete_old_files(table_paths['mismatch_punches_df_path'])
delete_old_files(table_paths['total_punches_punches_df_path'])
# delete_old_files(table_paths['total_pytotpun_punches_df_path'])
# delete_old_files(table_paths['actual_punches_df_path'])

delete_old_files(table_paths['mismatch_report_path'])
if pg_data_flag == True:
    print("pg data is true!")
    server_df = server_collect_db_data(g_first_path)
    client_df = client_collect_db_data(g_first_path)
    if client_df is not None:
        create_wdtest(server_df,client_df,g_first_path)
if process_mode_flag == True:
    print("process data is true")        
    db_check_flag = test_db_len(g_current_path)
    print("db check flag: ",db_check_flag)
    if db_check_flag !=0:
        mismatch_flag,mismatch_df,processed_punches,mode_1_only_df = punch_mismatch(g_current_path)
        mismatch_df.to_csv('mismatch_df_after_punchmismatch.csv',index=False)
        # processed_punches.to_csv('processed_punches_after_punch_mistmatch.csv',index=False)
        print("punch check flag: ",mismatch_flag)
        print("mismatch df: ",mismatch_df)
        print("mismatch flag: ",mismatch_flag)

        if isinstance(db_check_flag, dict) and mismatch_flag == 1:
            muster_df,muster_del_filtered = generate_muster(db_check_flag,g_current_path)
            # muster_df.to_csv('muster_df_after_generate_muster.csv',index=False)
            # muster_del_filtered.to_csv('muster_del_filtered_generate_muster.csv',index=False)
            punch_df = generate_punch(processed_punches,muster_del_filtered,g_current_path)
            create_final_csv(muster_df, punch_df,mismatch_df,g_current_path,mode_1_only_df)
            
        else:
            print("Either check empty_tables.txt or mismatch.csv")

# except Exception as e:
#     print(e)

# except IOError:
#     sys.exit()