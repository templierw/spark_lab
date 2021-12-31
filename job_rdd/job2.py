from Job import Job, Table
import numpy as np


def job_2():

    job = Table('machine_events', Job.sc).select(['job_id'])
    task_per_job = list(job.countByValue().values())

    return '\n'.join([
        f'mean: {np.mean(task_per_job)}', f'std: {np.std(task_per_job)}',
        f'max: {np.max(task_per_job)}', f'min: {np.min(task_per_job)}',
    ])

def main():
    job = Job('name', job_2)
    job.run()
    
if __name__ == "__main__":
    main()