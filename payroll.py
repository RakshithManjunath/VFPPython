from dbfread import DBF
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Read muster data from DBF file
muster_table = DBF('D:/ZIONtest/muster.dbf', load=True)
muster_df = pd.DataFrame(iter(muster_table))
