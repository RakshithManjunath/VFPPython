from dbf import Table,READ_WRITE

table = Table('D:/ZIONtest/pytotpun.dbf')
table.open(mode=READ_WRITE)

table.zap()
table.close()