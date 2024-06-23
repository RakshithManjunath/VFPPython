from dbfread import DBF

# Open and read the DBF file
table = DBF('D:/SWEETTOS/punches.dbf')

# Initialize a variable to store the last successfully processed index
last_index = -1

# Iterate through records with an index and handle potential errors
for index, record in enumerate(table):
    try:
        print(f"Index: {index}, Record: {record}")
        last_index = index
    except Exception as e:
        print(f"Error at index {index}: {e}")
        break

print(f"Last successfully processed index: {last_index}")
print(f"Total records processed: {last_index + 1}")

# If it stops at a specific record, inspect the record where it stops
if last_index + 1 < 6322:
    print(f"Inspecting record at index {last_index + 1}:")
    record_to_inspect = next(iter(DBF('D:/SWEETTOS/punches.dbf', encoding='utf-8')), None)
    if record_to_inspect:
        print(record_to_inspect)
    else:
        print("No record found at the specified index.")
