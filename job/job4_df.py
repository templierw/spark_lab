import sys
from job import Job, create_dataframe
import seaborn as sns
import matplotlib.pyplot as plt

def job_4():
    
    task_events = create_dataframe('task_events')
    print("selecting columns...")
    prievic = task_events.select(task_events.priority.cast('int'), task_events.event_type.cast('int'))

    evicted = prievic.select(prievic['*']).where((prievic.event_type == 2))
    total_evited = evicted.count()

    import pyspark.sql.functions as F

    final = evicted.groupBy('priority').count()
    final = final.withColumn('prob', F.round(final['count']/total_evited, 6)).sort('priority')

    pd_final = final.toPandas()
    
    sns.lineplot(x=pd_final['priority'], y='prob')

    plt.savefig('viz.png')
    plt.close()

    return final._jdf.showString(20, 20, False)

def main(name):
    Job(name, job_4, viz=True).run()
    
if __name__ == "__main__":
    main(sys.argv[1])