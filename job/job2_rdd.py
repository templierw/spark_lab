from lib import Job, Table, init
import sys
import numpy as np
import time


def job_2():

    job = Table('task_events', init(), -1, True)
    
    start = time.time()
    tasks_per_job = list(job.select(['job_id']).countByValue().values())

    mean = np.mean(tasks_per_job)
    std = np.std(tasks_per_job)
    max_v = np.max(tasks_per_job)
    min_v = np.min(tasks_per_job)

    # Compute high-end mean and low-end mean with the std derivation
    low_mean = mean - std if (mean - std >= min_v) else min_v
    high_mean = mean + std if (mean + std <= max_v) else min_v

    res = '\n'.join([
        f'mean: {mean}', f'std: {std}',
        f'max: {max_v}', f'min: {min_v}',
        f'low_mean: {low_mean}', f'high_mean {high_mean}'
    ])

    return res, round(time.time() - start, 2)

def main(name):
    job = Job(name, job_2)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])