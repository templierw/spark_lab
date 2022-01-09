from lib import Job, Table, init
import sys

def init(new):
    job = set()
    job.add(new)
    return [job, 1]

def merge(old, new):
    old[0].add(new)
    return [old[0], old[1] + 1]

def combine(c1, c2):
    u = c1[0].union(c2[0])
    return [u, c1[1]+c2[1]]

def job_3():

    res = Table('task_events', init())\
            .select(['scheduling_class','job_id'])\
            .combineByKey(init, merge, combine)\
            .mapValues(lambda x: (len(x[0]), x[1]))

    return '\n'.join(
        f'scheduling class [{s}], #job: {j}, #task: {t}' for \
            s, (j,t) in res.collect()
    )

def main(name):
    job = Job(name, job_3)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])