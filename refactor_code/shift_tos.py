from punch import generate_punch
from muster import generate_muster
import pandas as pd

def create_final_csv(muster_df,punch_df):
    punch_df['PDATE'] = pd.to_datetime(punch_df['PDATE'])
    merged_df = pd.merge(muster_df, punch_df, on=['TOKEN', 'PDATE'], how='outer')

    mask = merged_df['MUSTER_STATUS'] == ""
    merged_df.loc[mask, 'MUSTER_STATUS'] = merged_df.loc[mask, 'PUNCH_STATUS']
    merged_df = merged_df.rename(columns={"MUSTER_STATUS": "STATUS"})

    merged_df = merged_df.drop(['DATE_JOIN','DATE_LEAVE','PUNCH_STATUS'],axis=1)

    status_counts_by_empcode = merged_df.groupby('TOKEN')['STATUS'].value_counts().reset_index(name='COUNT')
    status_counts_by_empcode['COUNT'] = status_counts_by_empcode.apply(lambda row: row['COUNT'] / 2 if row['STATUS'] == 'A1' else row['COUNT'], axis=1)

    print(status_counts_by_empcode)

    merged_df.to_csv('./final.csv',index=False)

    # muster_counts = muster_counts.groupby(['TOKEN', 'MUSTER_STATUS']).size().unstack(fill_value=0)

    # muster_counts = muster_counts.merge(muster_counts, left_on='TOKEN', right_index=True)

muster_df = generate_muster()
punch_df = generate_punch()

create_final_csv(muster_df,punch_df)