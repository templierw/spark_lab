import os
import time
import numpy as np
from datetime import datetime as dt

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

    TABLES['machine_events'].rdd.unpersist()

    return res

"""
job2: 
"""

def job_2():
    job = TABLES['task_events'].select(['job_id'])
    task_per_job = list(job.countByValue().values())
    res = '\n'.join([
        f'mean: {np.mean(task_per_job)}', f'std: {np.std(task_per_job)}',
        f'max: {np.max(task_per_job)}', f'min: {np.min(task_per_job)}',
    ])

    return res

"""
job3: 
"""

def job_3():

    job_task_sched = TABLES['task_events'].select(['scheduling_class','job_id'])

    def init(new):
        job = set()
        job.add(new)
        return [job, 1]

    def merge(old, new):
        old[0].add(new)
        return [old[0], old[1] + 1]

    def combine(c1, c2):
        u = c1[0].union(c2[0])
        return [u, c1[1]+c2[1]]

    res = job_task_sched.combineByKey(init, merge, combine)\
                .mapValues(lambda x: (len(x[0]), x[1]))

    return '\n'.join(
        f'scheduling class [{s}], #job: {j}, #task: {t}' for \
            s, (j,t) in res.collect()
    )

"""
job4: 
"""

def job_4():
    rdd = TABLES['task_events'].select(['event_type', 'priority'])\
        .filter(lambda x: x[0] == '2')\
        .map(lambda x: int(x[1]))

    total_evicted = rdd.count()
    p = rdd.countByValue()

    print(f"Computing eviction probabilities for priorities]")
    return '\n'.join(
        f'priority: {pri} = {round(count/total_evicted, 6)}' \
            for pri, count in sorted(p.items())
        )

"""
job5: 
"""

def job_5():
    m_per_j = TABLES['task_events'].select(['job_id', 'machine_id'])\
           .groupByKey()\
           .mapValues(lambda x: len(set(x)))\
           .sortBy(lambda x: x[1], ascending=False)

    return '\n'.join(
        f'job [{job}], # machines = {machine}' \
            for job, machine in m_per_j.take(5)
    )

"""
job6.1
"""

def job_6_1():
    print("job 6.1: creating rdd for cpu request...")
    cpu_req = TABLES['task_events'].select(['job_id','task_index','cpu_request'])\
                     .filter(lambda x: x[2] != 'NA')\
                     .map(lambda x: ((x[0],x[1]),float(x[2])))
    TABLES['task_events'].rdd.unpersist()

    print("job 6.1: creating rdd for cpu usage...")
    cpu_us = TABLES['task_usage'].select(['job_id','task_index','cpu_rate'])\
                   .filter(lambda x: x[2] != 'NA')\
                   .map(lambda x: ((x[0],x[1]),float(x[2])))
    TABLES['task_usage'].rdd.unpersist()

    """
    inner functions
    """
    def avg_init(row):
        return (row[0], row[1], 1)

    def avg_merge(old, new):
        return (old[0]+new[0],old[1]+new[1],old[2]+1)

    def avg_cmb(old, new):
        return (old[0]+new[0],old[1]+new[1],old[2]+new[2])

    print("job 6.1: computing stats...")
    cpu_cons_avg = cpu_req.join(cpu_us).combineByKey(avg_init,avg_merge, avg_cmb)
    res = cpu_cons_avg.mapValues(lambda x: (round(x[0]/x[2],2),round(x[1]/x[2], 2)))\
                .sortBy(lambda x: x[1][0], ascending=False).take(10)

    return 'JOB | TASK | CPU REQ | CPU USAGE\n' + '\n'.join(
        f'{job} | {task} | {cpu_r} | {cpu_u}' for (job, task), (cpu_r, cpu_u) in res
        )

def main():
    timestamp = dt.now().strftime("%m.%d.%Y_%H:%M:%S")
    
    temp_file = open('tempres.txt', 'x+')
    blob = bucket.blob(f'jobs/job_{timestamp}_result.txt')

    jobs = [job_1, job_2, job_3, job_4, job_5]

    for n, job in enumerate(jobs):
        start = time.time()
        res = job()
        t = round(time.time() - start, 2)

        l1 = f'Job #{n+1} total time = {t}'
        output = f"{l1}\n{'-'*len(l1)}\n{res}\n\n"
        print(output)
        temp_file.write(output)

    temp_file.close()
    blob.upload_from_filename('tempres.txt')
    os.remove('tempres.txt')

if __name__ == "__main__":
    main()