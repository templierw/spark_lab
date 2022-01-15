import sys
from lib import Job, Table, init
import time

def job_5():

    sample = 0.5
    rdd = Table('task_events', init(), -1, sample=sample)
    
    start = time.time()
    m_per_j = rdd.select(['job_id', 'machine_id'])\
            .groupByKey()\
            .mapValues(lambda x: len(set(x)))\
            .sortBy(lambda x: x[1], ascending=False)

    res = '\n'.join(
        f'job [{job}], # machines = {machine}' \
            for job, machine in m_per_j.take(5)
    )

    return res, round(time.time() - start, 2)

def main(name):
    Job(name, job_5).run()
    
if __name__ == "__main__":
    main(sys.argv[1])