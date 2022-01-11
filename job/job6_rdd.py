import sys
from lib import Job, Table, init
import time
import matplotlib.pyplot as plt
from google.cloud import storage

def avg_init(row):
    return (row, 1)

def avg_merge(old, new):
    return (old[0]+new,old[1]+1)

def avg_cmb(old, new):
    return (old[0]+new[0],old[1]+new[1])

#for readability, cpu/mem request and usage is multiplied by 100
sanitize = lambda x: round(float(x)*100,4)

def job_6():

    sc = init()

    sample = 0.1
    bucket = storage.Client().get_bucket('wallbucket')
    te = Table('task_events', sc, -1, sample=sample)
    tu = Table('task_usage', sc, -1, sample=sample)
    plot_cpu = bucket.blob(f'job6_cpu_rdd_result.png')
    plot_mem = bucket.blob(f'job6_mem_rdd_result.png')
    start = time.time()

    """
    CPU CONSUMPTION
    """

    # Collect the cpu requested of each process by averaging on per task request,
    # and discard the Non Available values.
    cpu_requests = te.select(['job_id','cpu_request'])\
                        .filter(lambda x: x[1] != 'NA')\
                        .map(lambda x: (x[0],sanitize(x[1])))
    cpu_req_avg = cpu_requests.combineByKey(avg_init,avg_merge, avg_cmb)\
        .mapValues(lambda x: round(x[0]/x[1],3))

    # Same with the CPU rate
    cpu_used = tu.select(['job_id','cpu_rate'])\
                    .filter(lambda x: x[1] != 'NA')\
                    .map(lambda x: (x[0],sanitize(x[1])))
    cpu_used_avg = cpu_used.combineByKey(avg_init,avg_merge, avg_cmb)\
        .mapValues(lambda x: round(x[0]/x[1],3))

    #for each job we match its avg cpu request and usage,
    # then compute the delta REQUESTED - USAGE
    cpu_consumption = cpu_req_avg.join(cpu_used_avg)
    cons_delta = cpu_consumption.map(lambda x: round(x[1][0]-x[1][1], 4))

    cpu_time = time.time() - start

    data = cons_delta.collect()

    print(f"Job 6 cpu rdd ended [{cpu_time}], now plotting...")

    plt.hist(data)
    plt.title('Histogram of requested cpu \n minus used deltas')
    plt.xlabel('REQUESTED - USED')
    plt.savefig('viz.png')
    plot_cpu.upload_from_filename('viz.png')
    plt.close()

    """
    MEMORY CONSUMPTION
    """
    print("JOB 6 RDD: memory consumption")
    mem_start = time.time()

    # Collect the mem requested of each process by averaging on per task request,
    # and discard the Non Available values.
    mem_requests = te.select(['job_id','memory_request'])\
                        .filter(lambda x: x[1] != 'NA')\
                        .map(lambda x: (x[0],sanitize(x[1])))
    mem_req_avg = mem_requests.combineByKey(avg_init,avg_merge, avg_cmb)\
        .mapValues(lambda x: round(x[0]/x[1],3))

    # Same with the mem rate
    mem_used = tu.select(['job_id','canonical_memory_usage'])\
                    .filter(lambda x: x[1] != 'NA')\
                    .map(lambda x: (x[0],sanitize(x[1])))
    mem_used_avg = mem_used.combineByKey(avg_init,avg_merge, avg_cmb)\
        .mapValues(lambda x: round(x[0]/x[1],3))

    #for each job we match its avg mem request and usage,
    # then compute the delta REQUESTED - USAGE
    mem_consumption = mem_req_avg.join(mem_used_avg)
    cons_delta = mem_consumption.map(lambda x: round(x[1][0]-x[1][1], 4))

    mem_time = time.time() - mem_start

    data = cons_delta.collect()

    print(f"Job 6 mem rdd ended [{mem_time}], now plotting...")

    plt.hist(data)
    plt.title('Histogram of requested memory \n minus used deltas')
    plt.xlabel('REQUESTED - USED')
    plt.savefig('viz2.png')
    plot_mem.upload_from_filename('viz2.png')
    plt.close()

    return f"PLOT ONLY\n cpu time: {cpu_time}\n memory time: {mem_time}", cpu_time + mem_time

def main(name):
    Job(name, job_6).run()
    
if __name__ == "__main__":
    main(sys.argv[1])