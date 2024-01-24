import pandas as pd
from dbfread import DBF

def test_db_len():
    dated_dbf = './dated.dbf'
    dated_table = DBF(dated_dbf, load=False) 
    dated_num_records = len(dated_table)

    muster_dbf = './muster.dbf'
    muster_table = DBF(muster_dbf, load=False) 
    muster_num_records = len(muster_table)

    holmast_dbf = './holmast.dbf'
    holmast_table = DBF(holmast_dbf, load=False) 
    holmast_num_records = len(holmast_table)

    punches_dbf = './punches.dbf'
    punches_table = DBF(punches_dbf, load=False) 
    punches_num_records = len(punches_table)

    lvform_dbf = './lvform.dbf'
    lvform_table = DBF(lvform_dbf, load=False) 
    lvform_num_records = len(lvform_table)

    with open('./empty_tables.txt', 'w') as file:
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

    if dated_num_records != 0 and muster_num_records != 0 and holmast_num_records != 0 and lvform_num_records != 0:
        return 1
    else:
        return 0
