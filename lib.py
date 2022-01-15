from datetime import datetime as dt
import os
import subprocess
import pandas as pd

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from google.cloud import storage

"""
The bucket containing the data for the analysis
"""
BASE_URL = "gs://clusterdata-2011-2"
data = lambda table: f"{BASE_URL}/{table}"

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

"""
Data downloader

A service to handle the interaction with gsutil
to download N data files and work locally
"""

LOCALDATA_PATH = "./local_data/"
EXEC = "gsutil"

from pathlib import Path

def download(tableName, nbFilesMax):
    # Check if exists
    if (tableName not in HEADERS.keys()):
        print(f"{tableName} is not marked as available.")
        return -1
    else:
        print(f"Will download at most {nbFilesMax} file(s) from {tableName}.")

    
    Path(f"{LOCALDATA_PATH}/{tableName}").mkdir(parents=True, exist_ok=True)

    # Get all parts of the table in buckets
    try:
        print(f"Polling bucket {data(tableName)}")
        sc = subprocess.Popen(
            [EXEC, "ls", data(tableName)],
            stderr=subprocess.PIPE, stdout=subprocess.PIPE
            )
        (sout, _) = sc.communicate()
        bucket = sout.decode("utf-8").split('\n')
        bucket.remove('')
        bucket = [b.replace(data(tableName), '') for b in bucket]

        if nbFilesMax > len(bucket):
            nbFilesMax = len(bucket)

    except FileNotFoundError:
        print(
            f"{EXEC} is not installed on your system. Cannot proceed with the downloading.")
        return -2

    for b in bucket[:nbFilesMax]:

        # Download it
        print(f'Dowloading [{b}]')
        subprocess.call(
            [EXEC, "cp", f'{data(tableName)}{b}', f"{LOCALDATA_PATH}/{tableName}/"],
            stderr=subprocess.PIPE, stdout=subprocess.PIPE
        )

    print(f"Successfully downloaded table [{tableName}] ({nbFilesMax}/{len(bucket)}).")
    return 0


"""
Spark session
"""

def init(context='local[*]'):
    return SparkContext(context)

"""
Create PySpark Dataframe
"""
def create_dataframe(table_name, exec_mode, sample):
    schema = StructType()
    spark = SparkSession.builder\
                .appName("pyspark_lab")\
                .getOrCreate()

    for h in HEADERS[table_name]:
        schema.add(h, StringType(), True)

    if exec_mode == -1:
        fs = data(table_name)

    else:
        download(table_name, exec_mode)
        fs = f'{LOCALDATA_PATH}/{table_name}'

    return spark.read.option('delimiter', ',').format("csv")\
            .schema(schema).load(fs).sample(fraction=sample).fillna('NA')

"""
Cloud Table Class
"""
class Table():

    def __init__(self, table_name, spark_context, exec_mode, sample) -> None:
        
        def preprocess(row: str):
            row = row.split(',')
            return ['NA' if x == "" else x for x in row] \
                     if "" in row else row

        #Cluster
        if exec_mode == -1:
            self.rdd = spark_context.textFile(data(table_name)).sample(False, sample).map(preprocess)
        
        elif exec_mode >= 1:
            if not os.path.isdir(f"{LOCALDATA_PATH}/{table_name}"):
                # Download file as it is not present
                status = download(table_name, exec_mode)
                if status != 0:
                    print(f"Could not download relevant data for {table_name}...")
                    return -1
            
            if os.path.isdir(f"{LOCALDATA_PATH}/{table_name}"):
                self.rdd = spark_context.textFile(f"{LOCALDATA_PATH}/{table_name}").map(preprocess)

            else:
                print(f"Could not create RDD for {table_name}...")
        #error
        else:
            pass

        
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

    def pprint(self, take=None):
        return pd.DataFrame(
            data=self.rdd.take(take) if take is not None else self.rdd.collect(),
            columns=self.header
        )


class Job:

    def __init__(self, job_name, job_fnc) -> None:
        
        bucket = storage.Client().get_bucket('wallbucket')
        prefix = f'jobs/{job_name}/{dt.now().strftime("%m.%d")}'
        self.res = bucket.blob(f'{prefix}_result.txt')
        self.fnc = job_fnc

    def run(self):
        res, time = self.fnc()

        l1 = f'total time = {time}'
        output = f"{l1}\n{'-'*len(l1)}\n{res}\n\n"
        print(output)
        self.res.upload_from_string(output)

# Init, merge and combine functions for the combineByKey
def count_init(row):
    return [1]

def count_merge(old, new):
    return [old[0]+1]

def count_cmb(old, new):
    return [old[0]+new[0]]