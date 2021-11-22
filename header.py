import re


def get_table_header(table):
    with open('data/schema.csv') as schema:
        header=  [
            line.split(',')[2].lower().replace(' ', '_')\
            for line in schema.readlines() if re.match(f'^{table}/*', line) 
            ]

    return header

def get_column_index(column_name, header):
    return header.index(column_name)
