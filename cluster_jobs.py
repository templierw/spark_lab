import os
import time
import numpy as np
from datetime import datetime

from pyspark import SparkContext
from google.cloud import storage

"""
Spark session & 
Google cloud storage bucket
"""
sc = SparkContext()
bucket = storage.Client().get_bucket('wallbucket')


"""
Tables Headers
(hard-coded to avoid submitted many files to the dataproc cluster)
"""
global HEADERS
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

"""
Cloud Table Class
"""
class Table():

    def __init__(self, table_name, spark_context, cache=True) -> None:
        
        def preprocess(row: str):
            row = row.split(',')
            return ['NA' if x == "" else x for x in row] \
                     if "" in row else row

        self.rdd = spark_context.textFile(f"gs://clusterdata-2011-2/{table_name}/").map(preprocess)
        if cache: self.rdd.cache()

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

global TABLES
TABLES = {table: Table(table, sc) for table in HEADERS.keys()}

"""
JOBS
"""

"""
job1: 
"""

def job_1():

    cpu_dist = TABLES['machine_events'].select(['cpus']).countByValue()

    res = '\n'.join(
        f'cpu type: {cpu_type[0]}, count: {value}' \
            for cpu_type, value in cpu_dist.items()
    )

    return res

def job_2():
    job = TABLES['task_events'].select(['job_id'])
    task_per_job = list(job.countByValue().values())
    res = '\n'.join([
        f'mean: {np.mean(task_per_job)}', f'std: {np.std(task_per_job)}',
        f'max: {np.max(task_per_job)}', f'min: {np.min(task_per_job)}',
    ])

    return res

def main(nb_job):
    timestamp = datetime.datetime.now().strftime("%m/%d/%Y_%H:%M:%S")
    
    temp_file = open('tempres.txt', 'x+')
    blob = bucket.blob(f'jobs/job_{timestamp}_result.txt')

    for n in range(1,nb_job+1):
        job = f'job{n}'
        start = time.time()
        res = job()
        t = round(time.time() - start, 2)

        l1 = f'Job #{n} total time = {t}\n'
        output = f"""
            {l1}\n
            {''*len(l1)}\n
            {res}\n\n
        """
        print(output)
        temp_file.write(output)

    temp_file.close()
    blob.upload_from_filename('tempres.txt')
    os.remove('tempres.txt')

