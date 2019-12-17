"""
Create a categorization od 
"""
path =  "/home/mutiga/Projects/data/arrears.csv"
import csv
from threading import Thread
import time
FIELDS = [
    'Arrears Days'
]
field_map = []
rst = []

def get_chunk(all_rows, num_workers=20):
    for i in range(0, len(all_rows), num_workers):
        yield all_rows[i:i + num_workers]

def generate_field_mappings(fields, data):
    """
    Get a mapping of the columns of interest
    :param fields: a list of the column headers
    :param data: list of the headers of the csv.
    :return:
    """
    mapping = {}
    fields = [f.replace(' ', '').lower() for f in fields]
    for index, field in enumerate(data):
        field_lower = field.replace(' ', '').lower()
        if field_lower in fields:
            # index = field_to_match.index(field_lower)
            mapping[field_lower] = index
    return mapping
 

def farmers_in_arrears(row, position, arrears_days=0):
    if row[position]:
        if int(row[position])  > arrears_days:
            rst.append(row[0])
           


def main():
    tt =  time.time()
    with open(path, 'r') as f:
        arrears =  csv.reader(f,delimiter=";")
        arr = []
        for row in arrears:
            arr.append(row)

    # Read the arrears csv in chunks
    field_map = generate_field_mappings(FIELDS,arr[0])
    # rst = []
    

    #perform a classification based on arrears in a multi thread
  
    # Bring in the data in chunks
    count = 0
    for chunk in get_chunk(arr[1:], 20):
        count += 1
        print(count)
        print("Chunk",len(chunk))
        threads = []
        for row in chunk:
            process = Thread(
                target=farmers_in_arrears,
                args=(row, field_map['arrearsdays'])
            )
            threads.append(process)
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    print("Total records", len(arr))
    print("Length of resultant dataset", len(rst))
    print("tt ->", time.time() - tt)



if __name__ == '__main__':
    main()