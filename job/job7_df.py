import sys
from lib import Job, create_dataframe
import time
from google.cloud import storage
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

def job_7():

    sample = 0.1

    bucket = storage.Client().get_bucket('wallbucket')
    te = create_dataframe('task_events', -1, sample=sample)
    tu = create_dataframe('task_usage', -1, sample=sample)
    plot = bucket.blob(f'job7_df_result.png')

    start = time.time()

    max_cpu = tu.select(
            tu.job_id,
            F.round(tu.maximum_cpu_rate.cast('double')*100 ,2).alias('mcrate')
        ).groupBy('job_id').max('mcrate')

    max_cpu = max_cpu.select(max_cpu.job_id, F.round('max(mcrate)',2).alias('mcrate'))
    max_cpu = max_cpu.filter(max_cpu.mcrate < 100)
    filtered_te = te.filter(te.event_type == '2').select(te.job_id).distinct()

    max_cpu_evt = filtered_te.join(max_cpu, on='job_id')

    final = max_cpu_evt.toPandas()

    end = round(time.time() - start, 2)
    print(f"Job 7 df ended [{end}], now plotting...")

    final.hist()
    plt.title('Histograms of evicted cpu \n w.r.t their maximum cpu rate')
    plt.xlabel('maximum cpu rate')
    plt.ylabel('count')
    plt.savefig('viz.png')
    plot.upload_from_filename('viz.png')
    plt.close()

    return "Plot only", end

def main(name):
    Job(name, job_7).run()
    
if __name__ == "__main__":
    main(sys.argv[1])