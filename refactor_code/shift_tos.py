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

    # Adjust the counts for 'A1'
    status_counts_by_empcode['COUNT'] = status_counts_by_empcode.apply(lambda row: row['COUNT'] / 2 if row['STATUS'] == 'A1' else row['COUNT'], axis=1)

    # Create a mask for rows with 'A1' as 'STATUS'
    a1_mask = status_counts_by_empcode['STATUS'] == 'A1'

    # Separate rows with 'A1' and without 'A1'
    a1_rows = status_counts_by_empcode[a1_mask]
    other_rows = status_counts_by_empcode[~a1_mask]

    # Merge 'A1' rows with 'PR' and 'AB' rows and update counts
    merged_rows = pd.merge(other_rows, a1_rows, on='TOKEN', suffixes=('_other', '_a1'), how='left')

    # Fill NaN values in 'COUNT_a1' with 0
    merged_rows['COUNT_a1'] = merged_rows['COUNT_a1'].fillna(0)

    # Calculate the final count
    merged_rows['COUNT'] = merged_rows['COUNT_other']

    # Update 'AB' rows
    merged_rows['COUNT'] = merged_rows.apply(lambda row: row['COUNT'] - row['COUNT_a1'] if row['STATUS_other'] == 'AB' else row['COUNT'], axis=1)

    # Update 'PR' rows
    merged_rows['COUNT'] = merged_rows.apply(lambda row: row['COUNT'] + row['COUNT_a1'] if row['STATUS_other'] == 'PR' else row['COUNT'], axis=1)

    # Drop unnecessary columns
    merged_rows = merged_rows[['TOKEN', 'STATUS_other', 'COUNT']]

    # Rename columns to match the original structure
    merged_rows.columns = ['TOKEN', 'STATUS', 'COUNT']

    # Print the modified DataFrame
    print(merged_rows)

    merged_df.to_csv('./final.csv',index=False)

muster_df = generate_muster()
punch_df = generate_punch()

create_final_csv(muster_df,punch_df)