from pyspark import SparkContext
import os
import re
import pandas as pd

class Table():

    def __init__(self, table_name, spark_context:SparkContext, cache=True) -> None:
        
        if os.path.isfile(f'local_data/{table_name}.csv'):
            def preprocess(row: str):
                row = row.split(',')
                
                return ['NA' if x == "" else x for x in row] \
                    if "" in row else row
            self.rdd = spark_context.textFile(f"./local_data/{table_name}.csv").map(preprocess)
            if cache: self.rdd.cache()

            self.set_table_header(table_name)

        else:
            print(f"Could not creadt RDD for {table_name}...")

    def set_table_header(self, table):
        with open('lib/schema.csv') as schema:
            self.header =  [
                line.split(',')[2].lower().replace(' ', '_')\
                for line in schema.readlines() if re.match(f'^{table}/*', line) 
                ]

    def select(self, column_names):

        idx = []

        for column_name in column_names:
            if column_name in self.header:
                idx.append(self.header.index(column_name))
            else:
                print(f"ERROR: undefined column [{column_name}]")
                return

        return self.rdd.map(lambda x: tuple(x[i] for i in idx))

    def pprint(self, take=None):
        return pd.DataFrame(
            data=self.rdd.take(take) if take is not None else self.rdd.collect(),
            columns=self.header
        )