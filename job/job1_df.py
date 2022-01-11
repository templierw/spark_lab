import sys
from lib import Job, create_dataframe
import time

def job_1():

    sample = 1

    df = create_dataframe('machine_events', -1, sample=sample)
    
    start = time.time()
    res = df.groupBy('cpus').count()._jdf.showString(20, 20, False)

    return res, round(time.time() - start, 2)

def main(name):
    job = Job(name, job_1)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])