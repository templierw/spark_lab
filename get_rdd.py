from pyspark import SparkContext
import header as h

#### Driver program

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")


# read the input file into an RDD[String]
wholeFile = sc.textFile("./data.csv")

# split each line into an array of items
entries = wholeFile.map(lambda x : x.split(','))

def filter_NA(row: list):
    if "" in row:
        return ['NA' if x == "" else x for x in row]
    else: return row

entries = entries.map(filter_NA)

print(entries.take(10))

# keep the RDD in memory
entries.cache()

cpus_index = h.get_column_index('CPUs', h.get_table_header('machine_events'))

cpus = entries.map(lambda x: x[cpus_index])
cpus_dist = cpus.countByValue()
for k,v in cpus_dist.items():
    print(k,v)