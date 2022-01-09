import sys
from lib import Job, create_dataframe
import time

def job_4():
    
    task_events = create_dataframe('task_events', -1, True)
    start = time.time()
    m_per_f = task_events.select(task_events.job_id, task_events.machine_id)

    return m_per_f.groupBy('job_id').count()._jdf.showString(20, 20, False), round(time.time() - start, 2)

def main(name):
    Job(name, job_4).run()
    
if __name__ == "__main__":
    main(sys.argv[1])