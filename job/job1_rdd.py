import sys
from lib import Job, Table, init
import time

def job_1():

    sample = 1
    data = Table('machine_events', init(), -1,sample=sample)
    
    start = time.time()

    cpu_dist = data.select(['cpus']).countByValue()

    res = '\n'.join(
        f'cpu type: {cpu_type[0]}, count: {value}' \
            for cpu_type, value in cpu_dist.items()
    )
    
    return res, round(time.time() - start, 2)


def main(name):
    job = Job(name, job_1)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])