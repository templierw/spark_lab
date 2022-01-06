from job import Job, Table, init
import sys
import numpy as np


def job_2():

    job = Table('task_events', init()).select(['job_id'])
    task_per_job = list(job.countByValue().values())

    return '\n'.join([
        f'mean: {np.mean(task_per_job)}', f'std: {np.std(task_per_job)}',
        f'max: {np.max(task_per_job)}', f'min: {np.min(task_per_job)}',
    ])

def main(name):
    job = Job(name, job_2)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])