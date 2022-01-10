import sys
import seaborn as sns
import matplotlib.pyplot as plt
from lib import *
import time
from google.cloud import storage

def job_8():

    sc = init()
    te = Table('task_events', sc, -1, True)
    tc = Table('task_constraints', sc, -1, True)
    bucket = storage.Client().get_bucket('wallbucket')
    plot = bucket.blob(f'job8.rdd_result.png')

    sample = 0.01

    start = time.time()

    # Select first SUBMIT transition for each job
    submit_status = te.select(['job_id','task_index','event_type', 'time'])\
        .sample(False, sample)\
        .filter(lambda x: x[1] in ['0'])\
    .map(lambda x: (x[0], float(x[2])))

    rdd_submit = submit_status.groupByKey().mapValues(lambda x: round(sum(x)/len(x), 3))
    
    # Select first SUBMIT transition for each job
    outpending_status = te.select(['job_id','task_index','event_type', 'time'])\
        .sample(False, sample)\
        .filter(lambda x: x[1] in ['1', '3', '5', '6'])\
    .map(lambda x: (x[0], float(x[2])))

    rdd_out = outpending_status.groupByKey().mapValues(lambda x: round(sum(x)/len(x), 3))

    rdd_deltatimes = rdd_submit.join(rdd_out).map(lambda x: (x[0], x[1][1] - x[1][0]))

    # Load the task_constraints table

    # Selects each occurence of constraint registered for each process
    task_constraints_per_jobtask = tc.select(['job_id', 'task_index', 'time']).map(lambda x: ((x[0],x[1]), x[2]))

    # Counts the total number of constraints for each process
    rdd_number_task_constraints_per_jobtask = task_constraints_per_jobtask.combineByKey(count_init, count_merge, count_cmb).map(lambda x: ((x[0][0]), x[1][0]))

        # Join the delta time and the number of constraints in one RDD
    rdd_delta_constraints = rdd_deltatimes.join(rdd_number_task_constraints_per_job).filter(lambda x: x[1][1] < 50)

    # Create the list of values from the last RDD
    rdd_to_map = rdd_delta_constraints.sample(False, sample * 2).collectAsMap()
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