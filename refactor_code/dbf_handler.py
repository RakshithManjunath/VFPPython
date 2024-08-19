import dbf
import pandas as pd

def record_to_dict(record, field_names):
    result = {}
    for field in field_names:
        result[field] = record[field]
    return result

def dbf_2_df(filename,type):
    if type == "csv":
        df=pd.read_csv(filename)
        return len(df)
    else:
        with dbf.Table(filename) as table:
            field_names = table.field_names
            data = [record_to_dict(record, field_names) for record in table]
        if type == "len":
            return len(data)
        df = pd.DataFrame(data)
        return df

# print(dbf_2_df(filename="D:/ZIONtest/punches.dbf",type="dataframe"))
# print(dbf_2_df(filename="D:/ZIONtest/punches.dbf",type="len"))