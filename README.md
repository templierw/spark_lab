# Analyzing data with spark

Vincent Aubriot, William Templer

## Contents of the archive

You will find enclosed:

- The file lab_new.ipynb, that contains the analysis conducted over the data on a local computer (raw data, visualizations and interpretations), and the associated python code.
- lib.py that contains utilities functions described on the next part.
- the run_job.sh script that is in charge of creating a cluster on which the jobs will be executed, launching each version of each job (either using a RDD or a DataFrame to store the data and process the computations), and finally cleaning up the cluster at the end.
- the job folder, with: the job.py, copy of the library file, and two files per analysis described on the next part (one for each type of implementation: RDD or DataFrame).
- And finally, in results, you will find the logs and visualizations that we obtained by processing the jobs on the cloud.

## Conducted analysis

For this project, we have conducted the following analysis on a local computer with a subset of the data :

- Distribution of the machines according to their CPU capacity

- Number of tasks that compose a job (average, standard deviation, and extremums)

- Distribution of the number of jobs and tasks per scheduling class

- Probability for a low-priority process of being evicted

- Number of machines where tasks from a single job are running

- Study of the relationship between requests and real usage metrics for the CPU and Memory values

- Correlations between peaks of high-resource consumption and task evictions

- Influence of the number of constraints applied on a task over the time spent in PENDING state

Apart from the last task that was processed using DataFrames, all of the above were studied using RDDs.

Those tasks were computed on local computers with a small subset of the original data and one worker, and on a cloud instance that has a direct access to the bucket, and is able to run 5 workers in parallel.

In order to properly conduct our analysis, we have written a library that contains several utilities:

- A download function that handles the interactions with gsutil, and allows the user to download either a part or the entierety of a table passed as argument.
  
- A create_dataframe function that centralizes the proper dataframe creation from a table with the correct headers associated.

- A Table class that automatically handles the downloading of the polled table (with the first function above), creates the associated RDD and puts it in cache. It also contains a select function to get specific columns from the RDD, and a "pretty print" to properly display the contents of a table.

- A class Job dedicated to run the tasks in the cloud. It defines the bucket that shall be used to store data and the name of the logs at initialization, and on run it computes the time taken to process the task, and returns the metrics, the logs and the png if there exists a visualization.

We also provide an implementation of some of the jobs as dataframes, in order to compare their performances.

## Our observations

As we experienced our implementation on both a local machine and a cloud instance, we noticed multiple differences.

As we expected, running our implementation on a local machine does not allows us to treat a large amount of data, for multiple reasons: this requires to download a large amount of data to have access to one table, and the fact of running the code on one machine not really designed for this purpose, like our laptops, greatly limitates the number of workers that can run in parallel. 

Thus, it takes a large amount of time to treat more than one part of a large table.

On the other side, running the tasks on a Google Cloud instance gives several advantages that reduces the processing time by far, and allows us to treat more data at once.

GCP allows to copy seamlessly the tables from the public bucket to ours, thus removing the downloading delay (not really part of the processing time in itself, but still important to consider).

In addition, running in the cloud allows to use more workers on different machines. This means that more computing power is allocated to RDD manipulations, such as the combine, join and sort operation that often heavily consumes resources.

All of this allows to process the analysis on the entirety of the tables, allowing us to print real statistics valid on the full dataset, instead of doing assumptions from a small selection that does not represents the reality of the situation.