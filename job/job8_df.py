from lib import *
import sys
import time

import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, min
from pyspark.sql import SparkSession

def job_8():

    te = create_dataframe('task_events', -1 , True)
    tc = create_dataframe('task_constraints', -1 , True)
    bucket = storage.Client().get_bucket('wallbucket')
    plot = bucket.blob(f'job8.df_result.png')

    start = time.time()

    # Select the first timestamp at which all processes enter in PENDING state
    # Only the first wait period in this state interests us
    submit_status = te.select(te.job_id,te.task_index,te.event_type,te.time)\
        .filter(te.event_type == '0').select(te.job_id,te.task_index,te.time)\
        .withColumnRenamed('time', 'time_start_pending')

    inpending = submit_status.groupBy(['job_id', 'task_index']).agg(min('time_start_pending').alias('time_start_pending'))

    # Select the first timestamp at which all processes exit the PENDING state
    outpending_status = te.select(te.job_id,te.task_index,te.event_type,te.time)\
        .filter(te.event_type.isin(['1', '3', '5', '6'])).select(te.job_id,te.task_index,te.time)\
        .withColumnRenamed('time', 'time_end_pending')
        
    outpending = outpending_status.groupBy(['job_id', 'task_index']).agg(min('time_end_pending').alias('time_end_pending'))

    # Join both sanitized dataframes together on job id and task id
    fullpending = inpending.join(outpending, ['job_id', 'task_index'])

    # Compute the delta for each occurence (time spent in pending state computed from both time_start_pending and time_end_pending)
    fullpending_with_delta = fullpending.withColumn('delta_time', col('time_end_pending') - col('time_start_pending'))
    fullpending_with_delta = fullpending_with_delta.select('job_id', 'delta_time')

    # Compute the mean delta time for each job
    fullpending_with_delta = fullpending_with_delta.drop('task_index').groupBy('job_id').mean('delta_time')

    # compute the mean number of constraints for each job
    cons_jt = tc.select(tc.job_id, tc.task_index)
    cons_jt = cons_jt.groupBy(cons_jt.job_id, cons_jt.task_index).count()
    cons_jt = cons_jt.drop('task_index').groupBy('job_id').mean('count')

    # Join the number of constraints with the delta time dataframe
    full_df = fullpending_with_delta.join(cons_jt, on='job_id')
    full_df = full_df.filter(full_df['avg(count)'] < 50)

    data = full_df.select('avg(delta_time)', 'avg(count)').toPandas()
    
    end = round(time.time() - start, 2)
    print(f"Job 8 df ended [{end}], now plotting...")

    g = sns.scatterplot(data=data, x='avg(count)', y="avg(delta_time)")

    g.set_xlabel("Number of constraints")
    g.set_ylabel("Delta time")
    g.set_title("Time spent on PENDING state\n depending on the number of constraints")
    
    plt.savefig('viz.png')
    plot.upload_from_filename('viz.png')
    plt.close()

    res = "PLOT only"

    return res, end


def main(name):
    job = Job(name, job_8)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])