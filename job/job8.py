from job import Job, Table, init
import sys

import seaborn as sns
import matplotlib.pyplot as plt
colors = sns.color_palette('viridis')
plt.rcParams['figure.figsize'] = [12, 8]
plt.rcParams['figure.dpi'] = 100

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
from pyspark.sql import SparkSession

def job_7():

    sc = init()
    spark = SparkSession(sc)
    task_events = Table('task_events', sc)

    # Select first SUBMIT transition for each job
    col1 = ['job_id','task_index','event_type', 'time']
    submit_status = task_events.select(col1).filter(lambda x: x[2] in ['0'])

    # Create a dataframe from the gathered data
    df = spark.createDataFrame(submit_status, col1).withColumnRenamed('time', 'time_start_pending')

    # Partition the dataframe by job id and task id, and order each group by timestamp
    col2 = ['job_id', 'task_index']
    window = Window.partitionBy([col(x) for x in col2]).orderBy(df['time_start_pending'])

    # Compute the rank of each row in each partition, and filter to keep only the first row of each partition,
    # so that the first occurence of submit transition is kept for each process (entrance of the process in pending status)
    inpending = df.select(
            '*', rank().over(window).alias('rank')
        ).filter(col('rank') == 1)

    outpending_status = task_events.select(col1).filter(
            lambda x: x[2] in ['1', '3', '5', '6']
        )

    # Create a dataframe from the gathered data
    df = spark.createDataFrame(outpending_status, col1).withColumnRenamed('time', 'time_end_pending')

    # Partition the dataframe by job id and task id, and order each group by timestamp
    window = Window.partitionBy([col(x) for x in col2]).orderBy(df['time_end_pending'])

    # Compute the rank of each row in each partition, and filter to keep only the first row of each partition,
    # so that the first occurence of schedule-fail-kill or lost transition is kept for each process (exit from the process in pending status)
    outpending = df.select(
            '*', rank().over(window).alias('rank')
        ).filter(col('rank') == 1)

    # Join both sanitized dataframes together on job id and task id
    fullpending = inpending.join(outpending, ['job_id', 'task_index'])

    # Compute the delta for each occurence (time spent in pending state computed from both time_start_pending and time_end_pending)
    fullpending_with_delta = fullpending.withColumn('delta_time', col('time_end_pending') - col('time_start_pending'))

    # Load the task_constraints table
    task_constraints = Table('task_constraints', spark_context=sc)

    # Selects each occurence of constraint registered for each process
    task_constraints_per_jobtask = task_constraints.select(['job_id', 'task_index', 'time']).map(lambda x: ((x[0],x[1]), x[2]))

    # Inti, merge and combine functions for the combineByKey
    def avg_init(row):
        return [1]

    def avg_merge(old, new):
        return [old[0]+1]

    def avg_cmb(old, new):
        return [old[0]+new[0]]

    # Counts the total number of constraints for each process
    number_task_constraints_per_jobtask = task_constraints_per_jobtask.combineByKey(
            avg_init, avg_merge, avg_cmb
        ).map(lambda x: (x[0][0], x[0][1], x[1][0]))

    # Convert this RDD to a dataframe
    cNames = ["job_id", "task_index", "nb_constraints"]
    df_number_task_constraints_per_jobtask = number_task_constraints_per_jobtask.toDF(cNames)
        
    # Join the number of constraints with the delta time dataframe
    full_dataframe = fullpending_with_delta.join(df_number_task_constraints_per_jobtask, ['job_id', 'task_index'])
    
    sns.scatterplot(data=full_dataframe.toPandas(), x="nb_constraints", y="delta_time")

    plt.savefig('viz.png')

    return full_dataframe.toPandas()

def main(name):
    job = Job(name, job_7, viz=True)
    job.run()
    
if __name__ == "__main__":
    main(sys.argv[1])