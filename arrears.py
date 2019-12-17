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
        print(row[position])
        if int(row[position])  > arrears_days:
            return row[0]


def main():
    tt =  time.time()
    with open(path, 'r') as f:
        arrears =  csv.reader(f,delimiter=";")
        arr = []
        for row in arrears:
            arr.append(row)

    # Read the arrears csv in chunks
    field_map = generate_field_mappings(FIELDS,arr[0])
    rst = []

    #perform a classification based on arrears in a single core thread
    for each in arr[1:]:
        record = farmers_in_arrears(each, field_map['arrearsdays'])
        if(record):
            rst.append(record)
    
    print("Total records", len(arr))
    print("Length of resultant dataset", len(rst))

    print("tt ->", time.time() - tt)



if __name__ == '__main__':
    main()