import sys
from lib import Job, Table, init
import time

def avg_init(row):
    return (row[0], row[1], 1)

def avg_merge(old, new):
    return (old[0]+new[0],old[1]+new[1],old[2]+1)

def avg_cmb(old, new):
    return (old[0]+new[0],old[1]+new[1],old[2]+new[2])


def job_6_1():

    sc = init()

    rdd =  Table('task_events', sc, 100, True)
    start = time.time()

    cpu_req = rdd.select(['job_id','task_index','cpu_request'])\
        .filter(lambda x: x[2] != 'NA')\
        .map(lambda x: ((x[0],x[1]),float(x[2])))

    cpu_us = rdd.select(['job_id','task_index','cpu_rate'])\
        .filter(lambda x: x[2] != 'NA')\
        .map(lambda x: ((x[0],x[1]),float(x[2])))

    cpu_cons_avg = cpu_req.join(cpu_us).combineByKey(avg_init,avg_merge, avg_cmb)
    res = cpu_cons_avg.mapValues(
        lambda x: (round(x[0]/x[2],2),round(x[1]/x[2], 2))
        )\
        .sortBy(lambda x: x[1][0], ascending=False).take(10)

    res = 'JOB | TASK | CPU REQ | CPU USAGE\n' + '\n'.join(
        f'{job} | {task} | {cpu_r} | {cpu_u}' for (job, task), (cpu_r, cpu_u) in res
        )

    return res, round(time.time() - start, 2)

def main(name):
    Job(name, job_6_1).run()
    
if __name__ == "__main__":
    main(sys.argv[1])