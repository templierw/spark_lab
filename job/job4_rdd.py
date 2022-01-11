import sys
from lib import Job, Table, init
import time

def job_4():

    sample=1
    rdd = Table('task_events', init(), -1, sample=sample)
    
    start = time.time()

    rdd = rdd.select(['event_type', 'priority'])\
        .filter(lambda x: x[0] == '2')\
        .map(lambda x: int(x[1]))

    total_evicted = rdd.count()
    p = rdd.countByValue()

    res = '\n'.join(
        f'priority: {pri} = {round(count/total_evicted, 6)}' \
            for pri, count in sorted(p.items())
        )

    return res, round(time.time() - start, 2)

def main(name):
    Job(name, job_4).run()
    
if __name__ == "__main__":
    main(sys.argv[1])