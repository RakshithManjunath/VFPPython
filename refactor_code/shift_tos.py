from punch import generate_punch
from muster import generate_muster
import pandas as pd

def create_final_csv(muster_df,punch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    merged_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='outer')

    mask = merged_df['MUSTER_STATUS'] == ""
    merged_df.loc[mask, 'MUSTER_STATUS'] = merged_df.loc[mask, 'PUNCH_STATUS']

    merged_df = merged_df.drop(['DATE_JOIN','DATE_LEAVE','PUNCH_STATUS'],axis=1)
    merged_df.to_csv('./final.csv',index=False)

muster_df = generate_muster()
punch_df = generate_punch()

create_final_csv(muster_df,punch_df)