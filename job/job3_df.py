from lib import Job, create_dataframe
import pyspark.sql.functions as F
import sys
import time

def job_3():

    sample = 1
    te = create_dataframe('task_events', -1, sample=sample)
    
    start = time.time()
    jts = te.select(
        te.scheduling_class, te.job_id, te.task_index
    )

    res = jts.groupBy('scheduling_class').agg(
        F.countDistinct(jts.job_id), F.count(jts.task_index)
    ).orderBy(jts.scheduling_class)._jdf.showString(20, 20, False)
    
    return res, round(time.time() - start, 2)

def main(name):
    job = Job(name, job_3)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])