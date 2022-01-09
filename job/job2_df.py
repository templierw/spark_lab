from lib import Job, create_dataframe
import pyspark.sql.functions as F
import sys

def job_2():

    task_events = create_dataframe('task_events')
    task_per_event = task_events.groupBy('job_id').count()

    return task_per_event.select(
        F.round(F.mean('count'),2).alias('mean'), F.round(F.stddev('count'), 2).alias('std'),
        F.min('count').alias('min'), F.max('count').alias('max')
    )._jdf.showString(20, 20, False)

def main(name):
    job = Job(name, job_2)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])