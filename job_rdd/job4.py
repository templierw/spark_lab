import sys
from job_rdd.job import Job, Table

def job_4():
    rdd = Table('task_events', Job.sc)\
        .select(['event_type', 'priority'])\
        .filter(lambda x: x[0] == '2')\
        .map(lambda x: int(x[1]))

    total_evicted = rdd.count()
    p = rdd.countByValue()

    print(f"Computing eviction probabilities for priorities]")
    return '\n'.join(
        f'priority: {pri} = {round(count/total_evicted, 6)}' \
            for pri, count in sorted(p.items())
        )

def main(name):
    Job(name, job_4).run()
    
if __name__ == "__main__":
    main(sys.argv[1])