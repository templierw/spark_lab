import sys
from lib import Job, create_dataframe
import time
from google.cloud import storage
from pyspark.sql import functions as F
import matplotlib.pyplot as plt

def job_6():

    sample = 0.5
    bucket = storage.Client().get_bucket('wallbucket')
    te = create_dataframe('task_events', -1, sample=sample)
    tu = create_dataframe('task_usage', -1, sample=sample)
    plot_cpu = bucket.blob(f'job6.1.cpu_df_result.png')
    plot_mem = bucket.blob(f'job6.1.mem_df_result.png')

    start = time.time()

    cpu_req = te.select(
            te.job_id,
            F.round(te.cpu_request.cast('double')*100,4).alias('cpu_req')
        ).filter(te.cpu_request != 'NA')\
        .groupBy('job_id').mean('cpu_req')
    cpu_req = cpu_req.select(cpu_req.job_id, F.round('avg(cpu_req)', 4).alias('cpu_req'))

    cpu_us = tu.select(
                tu.job_id,
                F.round(tu.cpu_rate.cast('double')*100,4).alias('cpu_rate')
        ).filter(tu.cpu_rate != 'NA')\
        .groupBy('job_id').mean('cpu_rate')
    cpu_us = cpu_us.select(cpu_us.job_id, F.round('avg(cpu_rate)', 4).alias('cpu_rate'))

    cpu_cons = cpu_req.join(cpu_us, on='job_id')

    final = cpu_cons.withColumn('delta', F.round(cpu_cons.cpu_req - cpu_cons.cpu_rate, 2))

    res_0 = "### CPU CONS### \n"
    res_1 = f"Top 10 gluttonous: \n{final.sort('delta')._jdf.showString(10, 20, False)}\n"
    res_2 = f"Top 10 frugal: \n{final.sort('delta', ascending=False)._jdf.showString(10, 20, False)}\n"

    int_time = time.time() - start
    final = final.select(final.delta).sample(0.2).toPandas().hist()
    
    print("creating plot with subset")

    plt.title('Histograms of requested \n cpu minus used deltas')
    plt.xlabel('delta')
    plt.ylabel('count')
    plt.savefig('viz.png')
    plot_cpu.upload_from_filename('viz.png')
    plt.close()

    ### MEM USAGE ###

    print("memory usage")
    mem_start = time.time()

    mem_req = te.select(
            te.job_id,
            F.round(te.memory_request.cast('double')*100,4).alias('mem_req')
        ).filter(te.memory_request != 'NA')\
        .groupBy('job_id').mean('mem_req')
    mem_req = mem_req.select(mem_req.job_id, F.round('avg(mem_req)', 4).alias('mem_req'))

    mem_us = tu.select(
                tu.job_id,
                F.round(tu.canonical_memory_usage.cast('double')*100,4).alias('mem_us')
        ).filter(tu.canonical_memory_usage != 'NA')\
        .groupBy('job_id').mean('mem_us')
    mem_us = mem_us.select(mem_us.job_id, F.round('avg(mem_us)', 4).alias('mem_us'))

    mem_cons = mem_req.join(mem_us, on='job_id')

    final = mem_cons.withColumn('delta', F.round(mem_cons.mem_req - mem_cons.mem_us, 2))

    res_3 = "### MEM CONS ###\n"
    res_4 = f"Top 10 gluttonous: \n{final.sort('delta')._jdf.showString(10, 20, False)}\n"
    res_5 = f"Top 10 frugal: \n{final.sort('delta', ascending=False)._jdf.showString(10, 20, False)}\n"

    end = time.time()
    print("plotting memory")

    final.select(final.delta).sample(0.2).toPandas().hist()
    plt.title('Histograms of request memory minus used deltas')
    plt.xlabel('delta')
    plt.ylabel('count')
    plt.savefig('viz.png')
    plot_mem.upload_from_filename('viz.png')
    plt.close()

    res = res_0 + res_1 + res_2 + res_3 + res_4 + res_5

    return res, round((end - mem_start) + int_time, 2)

def main(name):
    Job(name, job_6).run()
    
if __name__ == "__main__":
    main(sys.argv[1])