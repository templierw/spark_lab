from lib import Job, create_dataframe
import pyspark.sql.functions as F
import sys
import time

def job_2():

    sample = 1
    te = create_dataframe('task_events', -1, sample=sample)
    
    start = time.time()
    task_per_job = te.groupBy('job_id').count()

    res = task_per_job.select(
        F.round(F.mean('count'),2).alias('mean'), F.round(F.stddev('count'), 2).alias('std'),
        F.min('count').alias('min'), F.max('count').alias('max')
    )._jdf.showString(20, 20, False)

    return res, round(time.time() - start, 2)

def main(name):
    job = Job(name, job_2)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])