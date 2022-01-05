import sys
from job_rdd.job import Job, Table

def job_5():
    m_per_j = Table('task_events', Job.sc)\
            .select(['job_id', 'machine_id'])\
            .groupByKey()\
            .mapValues(lambda x: len(set(x)))\
            .sortBy(lambda x: x[1], ascending=False)

    return '\n'.join(
        f'job [{job}], # machines = {machine}' \
            for job, machine in m_per_j.take(5)
    )

def main(name):
    Job(name, job_5).run()
    
if __name__ == "__main__":
    main(sys.argv[1])