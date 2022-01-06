import time
from datetime import datetime as dt

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from google.cloud import storage


"""
Spark session & 
Google cloud storage bucket
"""

def init():
    return SparkContext()

"""
Tables Headers
(hard-coded to avoid submitted many files to the dataproc cluster)
"""
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
    ],
    "task_constraints": [
        'time', 'job_id', 'task_index', 'comparison_operator', 
        'attribute_name', 'attribute_value'
    ]
}

fs = lambda table: f"gs://clusterdata-2011-2/{table}/"

def create_dataframe(table_name):
    schema = StructType()
    spark = SparkSession.builder.master("local[*]")\
                .appName("pyspark_lab")\
                .getOrCreate()

    for h in HEADERS[table_name]:
        schema.add(h, StringType(), True)

    return spark.read.option('delimiter', ',').format("csv")\
            .schema(schema).load(fs(table_name)).fillna('NA')

"""
Cloud Table Class
"""
class Table():

    def __init__(self, table_name, spark_context) -> None:
        
        def preprocess(row: str):
            row = row.split(',')
            return ['NA' if x == "" else x for x in row] \
                     if "" in row else row

        self.rdd = spark_context.textFile(fs(table_name)).map(preprocess)
        
        self.rdd.cache()

        self.header = HEADERS[table_name]

    def select(self, column_names):

        idx = []
        for column_name in column_names:
            if column_name in self.header:
                idx.append(self.header.index(column_name))
            else:
                print(f"ERROR: undefined column [{column_name}]")
                return

        return self.rdd.map(lambda x: tuple(x[i] for i in idx))

"""
tables
"""

class Job:

    def __init__(self, job_name, job_fnc, viz=False) -> None:
        
        bucket = storage.Client().get_bucket('wallbucket')
        prefix = f'jobs/{job_name}/{dt.now().strftime("%m.%d")}'
        self.res = bucket.blob(f'{prefix}_result.txt')
        self.viz = bucket.blob(f'{prefix}_plot.txt') if viz else None
        self.name = job_name
        self.fnc = job_fnc

    def run(self):
        start = time.time()
        res = self.fnc()

        l1 = f'total time = {round(time.time() - start, 2)}'
        output = f"{l1}\n{'-'*len(l1)}\n{res}\n\n"
        print(output)
        self.res.upload_from_string(output)
        if self.viz:
            self.viz.upload_from_filename('viz.png')