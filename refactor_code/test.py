import pandas as pd
from dbfread import DBF

def test_lvform_len():
    # Specify the file path
    dbf_file_path = 'D:/ZIONtest/lvform.dbf'

    # Check if the file is empty
    lvform_table = DBF(dbf_file_path, load=False)  # Load=False to avoid loading data
    num_records = len(lvform_table)

    if num_records == 0:
        print(f"The file '{dbf_file_path}' is empty. Setting default values.")
        
        # Set default values
        lvform_df = pd.DataFrame({
            'EMPCODE': [],
            'LV_ST': pd.to_datetime([]),  # Assuming LV_ST is a datetime column
            'LV_TYPE': []
        })
    else:
        print("Data exists")
