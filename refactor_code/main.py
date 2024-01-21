from punch import generate_punch
from muster import generate_muster
import pandas as pd

def filter_muster_df(muster_df):
    muster_df = muster_df.drop(['DATE_JOIN', 'DATE_LEAVE','PDATE'], axis=1)
    return muster_df

def filter_punch_df(punch_df):
    punch_df = punch_df.drop(['PDATE','INTIME1','OUTTIME1','INTIME2','OUTTIME2','INTIME3','OUTTIME3','INTIME4','OUTTIME4','INTIME','OUTTIME','TOTALTIME','REMARKS','ATTN_STATUS'],axis=1)
    return punch_df

def create_final_csv(muster_df,punch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    result_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='inner')

    # Select the desired columns
    result_df = result_df[['TOKEN', 'COMCODE', 'NAME', 'EMPCODE', 'EMP_DEPT', 'DEPT_NAME', 'EMP_DESI', 'DESI_NAME',
                        'DATE_JOIN', 'DATE_LEAVE', 'PDATE', 'ATT_STATUS', 'INTIME1', 'OUTTIME1', 'INTIME2', 'OUTTIME2',
                        'INTIME3', 'OUTTIME3', 'INTIME4', 'OUTTIME4', 'INTIME', 'OUTTIME', 'TOTALTIME', 'REMARKS']]
    result_df.to_csv('final.csv',index=False)

muster_df = generate_muster()
punch_df = generate_punch()

# final_muster_df = filter_muster_df(muster_df)
# final_punch_df = filter_punch_df(punch_df)

create_final_csv(muster_df,punch_df)