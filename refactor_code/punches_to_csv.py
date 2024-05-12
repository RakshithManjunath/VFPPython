import pandas as pd
from dbfread import DBF

punches_table = DBF("./punches.dbf", load=True)
punches_df = pd.DataFrame(iter(punches_table))

punches_df.to_csv("./wdtest_client.csv",index=False)