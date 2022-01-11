import sys
from lib import Job, create_dataframe
import pyspark.sql.functions as F
import time

def job_4():
    
    sample = 1
    task_events = create_dataframe('task_events', -1, sample=sample)
    
    start = time.time()

    prievic = task_events.select(task_events.priority.cast('int'), task_events.event_type.cast('int'))

    evicted = prievic.select(prievic['*']).where((prievic.event_type == 2))
    total_evited = evicted.count()

    final = evicted.groupBy('priority').count()
    final = final.withColumn('prob', F.round(final['count']/total_evited, 6)).sort('priority')

    return final._jdf.showString(20, 20, False), round(time.time() - start, 2)

def main(name):
    Job(name, job_4).run()
    
if __name__ == "__main__":
    main(sys.argv[1])