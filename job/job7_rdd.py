import sys
import seaborn as sns
import matplotlib.pyplot as plt
from lib import Job, Table, init
import time
from google.cloud import storage

def job_7():

    sc = init()
    tu = Table('task_usage', sc, -1, True)
    te = Table('task_events', sc, -1, True)
    bucket = storage.Client().get_bucket('wallbucket/jobs')
    plot = bucket.blob(f'job7.rdd_result.png')

    start = time.time()

    max_cpu_task = tu.select(['job_id', 'maximum_cpu_rate'])\
                        .mapValues(lambda x: round(float(x)*100))\
                        .reduceByKey(max)

    # Select the jobs that were evicted
    filtered_te = te.select(['job_id', 'event_type']).filter(lambda x: x[1] == '2').distinct()

    # Join both RDD to have the maximum CPU rate for each evicted job
    max_cpu_evt = filtered_te.join(max_cpu_task)

    final = max_cpu_evt.map(lambda x: x[1][1])

    data = final.collect()

    end = round(time.time() - start, 2)

    sns.histplot(data)
    plt.savefig('viz.png')
    plot.upload_from_filename('viz.png')
    plt.close()

    res = "PLOT only"

    return res, end

def main(name):
    Job(name, job_7).run()
    
if __name__ == "__main__":
    main(sys.argv[1])