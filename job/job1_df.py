import sys
from job import Job, create_dataframe

def job_1():

    df = create_dataframe('machine_events')
    return df.groupBy('cpus').count()._jdf.showString(20, 20, False)



def main(name):
    job = Job(name, job_1)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])