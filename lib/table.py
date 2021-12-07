from pyspark import SparkContext
import os
import pandas as pd

HEADERS = {
    "machine_events": ['time', 'machine_id', 'event_type', 'platform_id', 'cpus', 'memory'],
    "job_events": [
        'time', 'missing_info', 'job_id', 'event_type', 'user',
        'scheduling_class', 'job_name', 'logical_job_name'
    ],
    "task_events": [
        'time', 'missing_info', 'job_id', 'task_index', 'machine_id', 'event_type', 'user',
        'scheduling_class', 'priority', 'cpu_request', 'memory_request',
        'disk_space_request', 'different_machines_restriction'
    ],
    "task_usage": [
        'start_time', 'end_time', 'job_id', 'task_index', 'machine_id',
        'cpu_rate', 'canonical_memory_usage', 'assigned_memory_usage',
        'unmapped_page_cache', 'total_page_cache', 'maximum_memory_usage',
        'disk_i/o_time', 'local_disk_space_usage', 'maximum_cpu_rate',
        'maximum_disk_io_time', 'cycles_per_instruction', 'memory_accesses_per_instruction',
        'sample_portion', 'aggregation_type', 'sampled_cpu_usage'
    ]
}

class Table():

    def __init__(self, table_name, spark_context:SparkContext, cache=True) -> None:
        
        if os.path.isfile(f'local_data/{table_name}.csv'):
            def preprocess(row: str):
                row = row.split(',')
                
                return ['NA' if x == "" else x for x in row] \
                    if "" in row else row
            self.rdd = spark_context.textFile(f"./local_data/{table_name}.csv").map(preprocess)
            if cache: self.rdd.cache()

            self.header = HEADERS[table_name]

        else:
            print(f"Could not creadt RDD for {table_name}...")

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