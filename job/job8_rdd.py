import sys
import seaborn as sns
import matplotlib.pyplot as plt
from lib import *
import time
from google.cloud import storage

def job_8():

    sc = init()
    sample = 1.0
    te = Table('task_events', sc, -1, sample=sample)
    tc = Table('task_constraints', sc, -1, sample=sample)
    bucket = storage.Client().get_bucket('wallbucket')
    plot = bucket.blob(f'job8.rdd_result.png')

    start = time.time()

    # Select first SUBMIT transition for each job & task
    submit_status = te.select(['job_id', 'task_index', 'event_type', 'time'])\
        .filter(lambda x: x[2] in ['0'])\
        .map(lambda x: (x[0]+', '+x[1], float(x[3])))

    rdd_submit = submit_status.reduceByKey(min)

    # Select first OUT transition for each job & task
    outpending_status = te.select(['job_id', 'task_index', 'event_type', 'time'])\
        .filter(lambda x: x[2] in ['1', '3', '5', '6'])\
        .map(lambda x: (x[0]+', '+x[1], float(x[3])))

    rdd_out = outpending_status.reduceByKey(min)

    # Join everything and compute delta time for each job & task
    rdd_deltatimes = rdd_submit.join(rdd_out).map(lambda x: (x[0].split(', ')[0], x[1][1] - x[1][0]))

    # Average of the delta times for each job
    rdd_deltatimes = rdd_deltatimes.groupByKey().mapValues(lambda x: round(sum(x)/len(x), 3))

    # Selects each occurence of constraint registered for each process
    task_constraints_per_job = tc.select(['job_id', 'task_index'])\
        .map(lambda x: (x[0]+', '+x[1],1))\
        .reduceByKey(lambda a,b: a+b)\
        .map(lambda x: (x[0].split(', ')[0], x[1]))
    task_constraints_per_job = task_constraints_per_job.groupByKey().mapValues(lambda x: round(sum(x)/len(x), 3))

    rdd_delta_constraints = rdd_deltatimes.join(task_constraints_per_job).filter(lambda x: x[1][1] < 50)

    # Create the list of values from the last RDD
    rdd_to_map = rdd_delta_constraints.collectAsMap()
    deltatimes = [v[0] for v in rdd_to_map.values()]
    constraints = [v[1] for v in rdd_to_map.values()]

    end = round(time.time() - start, 2)

    print(f"Job 8 rdd ended [{end}], now plotting...")

    # And then pass it to seaborn to create a barplot to have a global view
    g = sns.scatterplot(x=constraints, y=deltatimes)

    g.set_xlabel("Number of constraints")
    g.set_ylabel("Delta time")

    g.set_title("Time spent on PENDING state \n depending on the number of constraints")
    
    plt.savefig('viz.png')
    plot.upload_from_filename('viz.png')
    plt.close()

    res = "PLOT only"

    return res, end

def main(name):
    Job(name, job_8).run()
    
if __name__ == "__main__":
    main(sys.argv[1])