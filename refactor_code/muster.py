# import pandas as pd
# from dbfread import DBF
# from test import file_paths
# from datetime import timedelta
# import dbf
# from dbf_handler import dbf_2_df
# from datetime import datetime
# from py_paths import g_current_path

# def safe_parse_date(date_str):
#     try:
#         return dbf.Date(date_str)
#     except (ValueError, TypeError):
#         return None
    
# import pandas as pd
# from dbfread import DBF
# from datetime import datetime, timedelta

# def generate_muster(db_check_flag, g_current_path, shift_master_df=None):
#     table_paths = file_paths(g_current_path)

#     with open(table_paths['gsel_date_path']) as file:
#         gseldate = pd.to_datetime(file.readlines()[0].strip())
#         gseldate_str = gseldate.strftime('%Y-%m-%d')

#     gseldate_date_format = datetime.strptime(gseldate_str, "%Y-%m-%d")
#     next_day = gseldate_date_format + timedelta(days=1)
#     is_last_day = gseldate_date_format.month != next_day.month

#     holmast_flag = lvform_flag = None
#     if isinstance(db_check_flag, dict) and ('optional_tables' in db_check_flag):
#         holmast_flag = db_check_flag['optional_tables'][0]
#         lvform_flag = db_check_flag['optional_tables'][1]

#     dated_table = DBF(table_paths['dated_dbf_path'], load=True)
#     start_date = dated_table.records[0]['MUFRDATE']
#     end_date = dated_table.records[0]['MUTODATE']
#     start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
#     end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
#     start_ts = pd.to_datetime(start_date)
#     end_ts = pd.to_datetime(end_date)

#     muster_table = DBF(table_paths['muster_dbf_path'], load=True)
#     muster_df = pd.DataFrame(iter(muster_table))
#     muster_df = muster_df[muster_df['SEC_STAFF'] == True].copy()
#     muster_df['DEL'] = muster_df['DEL'].fillna(False)

#     if 'DATE_LEAVE' in muster_df.columns:
#         muster_df['DATE_LEAVE'] = pd.to_datetime(muster_df['DATE_LEAVE'], errors='coerce')
#         muster_df['DATE_LEAVE_NORM'] = muster_df['DATE_LEAVE'].dt.normalize()
#         keep_mask = (muster_df['DEL'] == False) | muster_df['DATE_LEAVE_NORM'].between(start_ts, end_ts)
#         muster_df = muster_df[keep_mask].copy()
#         muster_df.drop(columns=['DATE_LEAVE_NORM'], inplace=True)
#     else:
#         muster_df = muster_df[muster_df['DEL'] == False].copy()

#     muster_df = muster_df.sort_values(by=['TOKEN'])
#     drange = pd.date_range(start=start_date, end=end_date, freq='D')

#     wo_col_map = {f'WO_{i}': d.strftime('%Y-%m-%d') for i, d in enumerate(drange, start=1)}
#     ashift_col_map = {f'ASHIFT_{i}': d.strftime('%Y-%m-%d') for i, d in enumerate(drange, start=1)}

#     selected_columns = muster_df.loc[:, wo_col_map.keys()].copy()
#     selected_columns.columns = [wo_col_map[c] for c in selected_columns.columns]
#     selected_columns.index = muster_df['TOKEN']

#     selected_columns_ashift = muster_df.loc[:, ashift_col_map.keys()].copy()
#     selected_columns_ashift.columns = [ashift_col_map[c] for c in selected_columns_ashift.columns]
#     selected_columns_ashift.index = muster_df['TOKEN']

#     token_dates = {}
#     for token, row in selected_columns.iterrows():
#         true_dates = [col for col, value in row.items() if bool(value)]
#         if true_dates:
#             token_dates[token] = true_dates

#     frames = []
#     for _, row in muster_df.iterrows():
#         token = row['TOKEN']
#         row_date_range = pd.date_range(start_date_str, end_date_str, freq='D')
#         frames.append(pd.DataFrame({
#             'TOKEN': token,
#             'COMCODE': row.get('COMCODE', None),
#             'NAME': row.get('NAME', None),
#             'EMPCODE': row.get('EMPCODE', None),
#             'EMP_DEPT': row.get('EMP_DEPT', None),
#             'DEPT_NAME': row.get('DEPT_NAME', None),
#             'EMP_DESI': row.get('EMP_DESI', None),
#             'DESI_NAME': row.get('DESI_NAME', None),
#             'DATE_JOIN': row.get('DATE_JOIN', None),
#             'DATE_LEAVE': row.get('DATE_LEAVE', None),
#             'PDATE': row_date_range,
#             'MUSTER_STATUS': "",
#             'SHIFT_STATUS': ""
#         }))
#     final_muster_df = pd.concat(frames, ignore_index=True)

#     final_muster_df['PDATE'] = pd.to_datetime(final_muster_df['PDATE'], errors='coerce')
#     final_muster_df['PDATE_norm'] = final_muster_df['PDATE'].dt.normalize()
#     final_muster_df['PDATE_str'] = final_muster_df['PDATE'].dt.strftime('%Y-%m-%d')

#     if holmast_flag != 0:
#         holidays_df = pd.read_csv(table_paths['holmast_csv_path'])
#         holidays_df['hol_dt'] = pd.to_datetime(holidays_df['hol_dt'], dayfirst=True).dt.date
#         filtered_holidays_df = holidays_df[(holidays_df['hol_dt'] >= start_date) & (holidays_df['hol_dt'] <= end_date)]
#         hol_map = dict(zip(filtered_holidays_df['hol_dt'], filtered_holidays_df['hol_type']))
#         mask_hol = final_muster_df['PDATE'].dt.date.isin(hol_map.keys())
#         final_muster_df.loc[mask_hol, 'MUSTER_STATUS'] = final_muster_df.loc[mask_hol, 'PDATE'].dt.date.map(hol_map)


#     for token, dates in token_dates.items():
#         mask = (final_muster_df['TOKEN'] == token) & (final_muster_df['PDATE_str'].isin(dates))
#         final_muster_df.loc[mask, 'MUSTER_STATUS'] = 'WO'

#     ashift_long = (
#         selected_columns_ashift.reset_index().rename(columns={'index': 'TOKEN'})
#         .melt(id_vars='TOKEN', var_name='PDATE_str', value_name='SHIFT_STATUS')
#     )
#     valid_mask = ashift_long['SHIFT_STATUS'].notna() & (ashift_long['SHIFT_STATUS'] != '') & (ashift_long['SHIFT_STATUS'] != False)
#     ashift_long = ashift_long[valid_mask].copy()
#     ashift_long['PDATE_norm'] = pd.to_datetime(ashift_long['PDATE_str'], errors='coerce').dt.normalize()

#     final_muster_df = final_muster_df.merge(
#         ashift_long[['TOKEN', 'PDATE_norm', 'SHIFT_STATUS']],
#         on=['TOKEN', 'PDATE_norm'], how='left', suffixes=('', '_ashift')
#     )
#     has_new_shift = final_muster_df['SHIFT_STATUS_ashift'].notna() & (final_muster_df['SHIFT_STATUS_ashift'] != '')
#     final_muster_df.loc[has_new_shift, 'SHIFT_STATUS'] = final_muster_df.loc[has_new_shift, 'SHIFT_STATUS_ashift']

#     sm_df = shift_master_df
#     if sm_df is None and 'shiftmast_csv_path' in table_paths:
#         try:
#             sm_df = pd.read_csv(table_paths['shiftmast_csv_path'])
#         except Exception:
#             sm_df = None

#     if sm_df is not None and not sm_df.empty:
#         shift_times = sm_df[['shcode', 'shift_st', 'shift_ed', 'workhrs']].copy()
#         shift_times['shcode'] = shift_times['shcode'].astype(str).str.strip()
#         for c in ['shift_st', 'shift_ed', 'workhrs']:
#             shift_times[c] = pd.to_numeric(shift_times[c], errors='coerce')
#         final_muster_df['SHIFT_STATUS'] = final_muster_df['SHIFT_STATUS'].astype(str).str.strip()
#         final_muster_df = final_muster_df.merge(
#             shift_times.rename(columns={
#                 'shcode': 'SHIFT_STATUS',
#                 'shift_st': 'SHIFT_ST',
#                 'shift_ed': 'SHIFT_ED',
#                 'workhrs': 'WORKHRS'
#             }),
#             on='SHIFT_STATUS', how='left'
#         )

#     blank_mask = final_muster_df['SHIFT_STATUS'].isna() | (final_muster_df['SHIFT_STATUS'].astype(str).str.strip() == '')
#     final_muster_df.loc[blank_mask, 'SHIFT_STATUS'] = 'XX'

#     for col, val in zip(['SHIFT_ST', 'SHIFT_ED', 'WORKHRS'], [8.0, 4.0, 8.0]):
#         final_muster_df[col] = final_muster_df[col].fillna(val)

#     # Extend muster_df with ASHIFT PDATE-wise info
#     frames = []
#     for token, row in selected_columns_ashift.iterrows():
#         for date_str, shift_code in row.items():
#             if pd.notna(shift_code) and shift_code != '':
#                 frames.append({
#                     'TOKEN': token,
#                     'PDATE': pd.to_datetime(date_str),
#                     'SHIFT_STATUS': str(shift_code).strip()
#                 })
#     muster_ashift_df = pd.DataFrame(frames)

#     # Join shift info to per-date entries in muster_df
#     if sm_df is not None and not sm_df.empty:
#         shift_times = sm_df[['shcode', 'shift_st', 'shift_ed', 'workhrs']].copy()
#         shift_times.columns = ['SHIFT_STATUS', 'SHIFT_ST', 'SHIFT_ED', 'WORKHRS']
#         muster_ashift_df = muster_ashift_df.merge(shift_times, on='SHIFT_STATUS', how='left')

#     muster_df = muster_df.merge(muster_ashift_df, on='TOKEN', how='left')
#     muster_df = muster_df[muster_df['SHIFT_STATUS'].notna()].copy()

#     def shift_hour_to_time(pdate, hour):
#         if pd.isna(pdate) or pd.isna(hour):
#             return pd.NaT
#         base_date = pd.to_datetime(pdate)
#         hr = int(hour)
#         minute = int(round((hour - hr) * 100))
#         return base_date + timedelta(hours=hr, minutes=minute)

#     def get_shift_times(row):
#         shift_st = shift_hour_to_time(row['PDATE'], row['SHIFT_ST'])
#         if row['SHIFT_STATUS'] == '3S' and not pd.isna(row['SHIFT_ED']):
#             shift_ed = shift_hour_to_time(row['PDATE'] + pd.Timedelta(days=1), row['SHIFT_ED'])
#         else:
#             shift_ed = shift_hour_to_time(row['PDATE'], row['SHIFT_ED'])
#         return pd.Series({'SHIFT_ST': shift_st, 'SHIFT_ED': shift_ed})

#     final_muster_df[['SHIFT_ST', 'SHIFT_ED']] = final_muster_df.apply(get_shift_times, axis=1)
#     if 'PDATE' in muster_df.columns and 'SHIFT_ST' in muster_df.columns and 'SHIFT_ED' in muster_df.columns:
#         muster_df[['SHIFT_ST', 'SHIFT_ED']] = muster_df.apply(get_shift_times, axis=1)

#     final_muster_df = final_muster_df.sort_values(by=['TOKEN', 'PDATE']).reset_index(drop=True)
#     final_muster_df.drop(columns=[c for c in ['PDATE_str', 'PDATE_norm', 'SHIFT_STATUS_ashift'] if c in final_muster_df.columns], inplace=True, errors='ignore')

#     final_muster_df.to_csv(table_paths['muster_csv_path'], index=False)
#     return final_muster_df, muster_df


import pandas as pd
from dbfread import DBF
from test import file_paths
from datetime import timedelta
import dbf
from dbf_handler import dbf_2_df
from datetime import datetime
from py_paths import g_current_path

def safe_parse_date(date_str):
    try:
        return dbf.Date(date_str)
    except (ValueError, TypeError):
        return None

import pandas as pd
from dbfread import DBF
from datetime import datetime, timedelta

def generate_muster(db_check_flag, g_current_path, shift_master_df=None):
    table_paths = file_paths(g_current_path)

    with open(table_paths['gsel_date_path']) as file:
        gseldate = pd.to_datetime(file.readlines()[0].strip())
        gseldate_str = gseldate.strftime('%Y-%m-%d')

    gseldate_date_format = datetime.strptime(gseldate_str, "%Y-%m-%d")
    next_day = gseldate_date_format + timedelta(days=1)
    is_last_day = gseldate_date_format.month != next_day.month

    holmast_flag = lvform_flag = None
    if isinstance(db_check_flag, dict) and ('optional_tables' in db_check_flag):
        holmast_flag = db_check_flag['optional_tables'][0]
        lvform_flag = db_check_flag['optional_tables'][1]

    dated_table = DBF(table_paths['dated_dbf_path'], load=True)
    start_date = dated_table.records[0]['MUFRDATE']
    end_date = dated_table.records[0]['MUTODATE']
    start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)

    muster_table = DBF(table_paths['muster_dbf_path'], load=True)
    muster_df = pd.DataFrame(iter(muster_table))
    muster_df = muster_df[muster_df['SEC_STAFF'] == True].copy()
    muster_df['DEL'] = muster_df['DEL'].fillna(False)

    if 'DATE_LEAVE' in muster_df.columns:
        muster_df['DATE_LEAVE'] = pd.to_datetime(muster_df['DATE_LEAVE'], errors='coerce')
        muster_df['DATE_LEAVE_NORM'] = muster_df['DATE_LEAVE'].dt.normalize()
        keep_mask = (muster_df['DEL'] == False) | muster_df['DATE_LEAVE_NORM'].between(start_ts, end_ts)
        muster_df = muster_df[keep_mask].copy()
        muster_df.drop(columns=['DATE_LEAVE_NORM'], inplace=True)
    else:
        muster_df = muster_df[muster_df['DEL'] == False].copy()

    muster_df = muster_df.sort_values(by=['TOKEN'])
    drange = pd.date_range(start=start_date, end=end_date, freq='D')

    wo_col_map = {f'WO_{i}': d.strftime('%Y-%m-%d') for i, d in enumerate(drange, start=1)}
    ashift_col_map = {f'ASHIFT_{i}': d.strftime('%Y-%m-%d') for i, d in enumerate(drange, start=1)}

    selected_columns = muster_df.loc[:, wo_col_map.keys()].copy()
    selected_columns.columns = [wo_col_map[c] for c in selected_columns.columns]
    selected_columns.index = muster_df['TOKEN']

    selected_columns_ashift = muster_df.loc[:, ashift_col_map.keys()].copy()
    selected_columns_ashift.columns = [ashift_col_map[c] for c in selected_columns_ashift.columns]
    selected_columns_ashift.index = muster_df['TOKEN']

    token_dates = {}
    for token, row in selected_columns.iterrows():
        true_dates = [col for col, value in row.items() if bool(value)]
        if true_dates:
            token_dates[token] = true_dates

    frames = []
    for _, row in muster_df.iterrows():
        token = row['TOKEN']
        row_date_range = pd.date_range(start_date_str, end_date_str, freq='D')
        frames.append(pd.DataFrame({
            'TOKEN': token,
            'COMCODE': row.get('COMCODE', None),
            'NAME': row.get('NAME', None),
            'EMPCODE': row.get('EMPCODE', None),
            'EMP_DEPT': row.get('EMP_DEPT', None),
            'DEPT_NAME': row.get('DEPT_NAME', None),
            'EMP_DESI': row.get('EMP_DESI', None),
            'DESI_NAME': row.get('DESI_NAME', None),
            'DATE_JOIN': row.get('DATE_JOIN', None),
            'DATE_LEAVE': row.get('DATE_LEAVE', None),
            'PDATE': row_date_range,
            'MUSTER_STATUS': "",
            'SHIFT_STATUS': ""
        }))
    final_muster_df = pd.concat(frames, ignore_index=True)

    final_muster_df['PDATE'] = pd.to_datetime(final_muster_df['PDATE'], errors='coerce')
    final_muster_df['PDATE_norm'] = final_muster_df['PDATE'].dt.normalize()
    final_muster_df['PDATE_str'] = final_muster_df['PDATE'].dt.strftime('%Y-%m-%d')

    # --- Holiday logic (existing) ---
    if holmast_flag != 0:
        holidays_df = pd.read_csv(table_paths['holmast_csv_path'])
        holidays_df['hol_dt'] = pd.to_datetime(holidays_df['hol_dt'], dayfirst=True).dt.date
        filtered_holidays_df = holidays_df[(holidays_df['hol_dt'] >= start_date) & (holidays_df['hol_dt'] <= end_date)]
        hol_map = dict(zip(filtered_holidays_df['hol_dt'], filtered_holidays_df['hol_type']))
        mask_hol = final_muster_df['PDATE'].dt.date.isin(hol_map.keys())
        final_muster_df.loc[mask_hol, 'MUSTER_STATUS'] = final_muster_df.loc[mask_hol, 'PDATE'].dt.date.map(hol_map)

    for token, dates in token_dates.items():
        mask = (final_muster_df['TOKEN'] == token) & (final_muster_df['PDATE_str'].isin(dates))
        final_muster_df.loc[mask, 'MUSTER_STATUS'] = 'WO'

    # --- LVFORM logic (new, no merge) ---
    if lvform_flag != 0:
        lvform_df = pd.read_csv(table_paths['lvform_csv_path'])

        # keep only required columns
        lvform_df = lvform_df[['empcode', 'lv_st', 'lv_type']].copy()

        # normalise types
        lvform_df['empcode'] = pd.to_numeric(lvform_df['empcode'], errors='coerce')
        lvform_df['empcode'] = lvform_df['empcode'].astype('Int64')  # allow NaNs but keep as int-like
        lvform_df['lv_st'] = pd.to_datetime(lvform_df['lv_st'], dayfirst=True).dt.date

        # use same date range as muster
        start_d = start_ts.date()
        end_d = end_ts.date()
        lvform_df = lvform_df[(lvform_df['lv_st'] >= start_d) & (lvform_df['lv_st'] <= end_d)]

        # build map: (empcode_str, date) -> lv_type
        lv_map = {
            (str(row.empcode), row.lv_st): row.lv_type
            for row in lvform_df.itertuples()
            if pd.notna(row.empcode) and row.lv_st is not None
        }

        def apply_leave(row):
            # use string version of EMPCODE to be safe w.r.t dtype
            key = (str(row['EMPCODE']), row['PDATE'].date())
            lv_val = lv_map.get(key)
            # if there is a leave entry, set it; else keep existing MUSTER_STATUS
            return lv_val if lv_val is not None else row['MUSTER_STATUS']

        if lv_map:
            final_muster_df['MUSTER_STATUS'] = final_muster_df.apply(apply_leave, axis=1)
    # --- end LVFORM logic ---

    ashift_long = (
        selected_columns_ashift.reset_index().rename(columns={'index': 'TOKEN'})
        .melt(id_vars='TOKEN', var_name='PDATE_str', value_name='SHIFT_STATUS')
    )
    valid_mask = ashift_long['SHIFT_STATUS'].notna() & (ashift_long['SHIFT_STATUS'] != '') & (ashift_long['SHIFT_STATUS'] != False)
    ashift_long = ashift_long[valid_mask].copy()
    ashift_long['PDATE_norm'] = pd.to_datetime(ashift_long['PDATE_str'], errors='coerce').dt.normalize()

    final_muster_df = final_muster_df.merge(
        ashift_long[['TOKEN', 'PDATE_norm', 'SHIFT_STATUS']],
        on=['TOKEN', 'PDATE_norm'], how='left', suffixes=('', '_ashift')
    )
    has_new_shift = final_muster_df['SHIFT_STATUS_ashift'].notna() & (final_muster_df['SHIFT_STATUS_ashift'] != '')
    final_muster_df.loc[has_new_shift, 'SHIFT_STATUS'] = final_muster_df.loc[has_new_shift, 'SHIFT_STATUS_ashift']

    sm_df = shift_master_df
    if sm_df is None and 'shiftmast_csv_path' in table_paths:
        try:
            sm_df = pd.read_csv(table_paths['shiftmast_csv_path'])
        except Exception:
            sm_df = None

    if sm_df is not None and not sm_df.empty:
        shift_times = sm_df[['shcode', 'shift_st', 'shift_ed', 'workhrs']].copy()
        shift_times['shcode'] = shift_times['shcode'].astype(str).str.strip()
        for c in ['shift_st', 'shift_ed', 'workhrs']:
            shift_times[c] = pd.to_numeric(shift_times[c], errors='coerce')
        final_muster_df['SHIFT_STATUS'] = final_muster_df['SHIFT_STATUS'].astype(str).str.strip()
        final_muster_df = final_muster_df.merge(
            shift_times.rename(columns={
                'shcode': 'SHIFT_STATUS',
                'shift_st': 'SHIFT_ST',
                'shift_ed': 'SHIFT_ED',
                'workhrs': 'WORKHRS'
            }),
            on='SHIFT_STATUS', how='left'
        )

    blank_mask = final_muster_df['SHIFT_STATUS'].isna() | (final_muster_df['SHIFT_STATUS'].astype(str).str.strip() == '')
    final_muster_df.loc[blank_mask, 'SHIFT_STATUS'] = 'XX'

    for col, val in zip(['SHIFT_ST', 'SHIFT_ED', 'WORKHRS'], [8.0, 4.0, 8.0]):
        final_muster_df[col] = final_muster_df[col].fillna(val)

    # Extend muster_df with ASHIFT PDATE-wise info
    frames = []
    for token, row in selected_columns_ashift.iterrows():
        for date_str, shift_code in row.items():
            if pd.notna(shift_code) and shift_code != '':
                frames.append({
                    'TOKEN': token,
                    'PDATE': pd.to_datetime(date_str),
                    'SHIFT_STATUS': str(shift_code).strip()
                })
    muster_ashift_df = pd.DataFrame(frames)

    # Join shift info to per-date entries in muster_df
    if sm_df is not None and not sm_df.empty:
        shift_times = sm_df[['shcode', 'shift_st', 'shift_ed', 'workhrs']].copy()
        shift_times.columns = ['SHIFT_STATUS', 'SHIFT_ST', 'SHIFT_ED', 'WORKHRS']
        muster_ashift_df = muster_ashift_df.merge(shift_times, on='SHIFT_STATUS', how='left')

    muster_df = muster_df.merge(muster_ashift_df, on='TOKEN', how='left')
    muster_df = muster_df[muster_df['SHIFT_STATUS'].notna()].copy()

    def shift_hour_to_time(pdate, hour):
        if pd.isna(pdate) or pd.isna(hour):
            return pd.NaT
        base_date = pd.to_datetime(pdate)
        hr = int(hour)
        minute = int(round((hour - hr) * 100))
        return base_date + timedelta(hours=hr, minutes=minute)

    def get_shift_times(row):
        shift_st = shift_hour_to_time(row['PDATE'], row['SHIFT_ST'])
        if row['SHIFT_STATUS'] == '3S' and not pd.isna(row['SHIFT_ED']):
            shift_ed = shift_hour_to_time(row['PDATE'] + pd.Timedelta(days=1), row['SHIFT_ED'])
        else:
            shift_ed = shift_hour_to_time(row['PDATE'], row['SHIFT_ED'])
        return pd.Series({'SHIFT_ST': shift_st, 'SHIFT_ED': shift_ed})

    final_muster_df[['SHIFT_ST', 'SHIFT_ED']] = final_muster_df.apply(get_shift_times, axis=1)
    if 'PDATE' in muster_df.columns and 'SHIFT_ST' in muster_df.columns and 'SHIFT_ED' in muster_df.columns:
        muster_df[['SHIFT_ST', 'SHIFT_ED']] = muster_df.apply(get_shift_times, axis=1)

    final_muster_df = final_muster_df.sort_values(by=['TOKEN', 'PDATE']).reset_index(drop=True)
    final_muster_df.drop(columns=[c for c in ['PDATE_str', 'PDATE_norm', 'SHIFT_STATUS_ashift'] if c in final_muster_df.columns], inplace=True, errors='ignore')

    final_muster_df.to_csv(table_paths['muster_csv_path'], index=False)
    return final_muster_df, muster_df

