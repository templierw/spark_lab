"""
Datadl.py

A small library handling the interaction with gsutil to download the data files
"""

import os
import subprocess
from os import listdir, mkdir
from os.path import isfile, join
import gzip
import shutil

EXEC = "gsutil"
BASE_URL = "gs://clusterdata-2011-2/"
AVAILABLE_TABLES = ["machine_events", "job_events",
                    "task_events", "task_usage", "task_constraints"]

LOCALDATA_PATH = "./local_data/"
TEMP_PATH = LOCALDATA_PATH + "tmp/"


def download(tableName, nbFilesMax):
    # Check if exists
    if (tableName not in AVAILABLE_TABLES):
        print(f"{tableName} is not marked as available. If you are sure it is and you need to use it, please edit the datadl library and add the name to the AVAILABLE_TABLES list.")
        return -1
    else:
        print(f"Will download at most {nbFilesMax} file(s) from {tableName}.")

    # Create tmp folder
    os.mkdir(TEMP_PATH)

    # Files on bucket
    bucket = []

    # List all parts of the table in bucket
    try:
        print("Polling bucket " + BASE_URL + tableName + '/')
        sc = subprocess.Popen([EXEC, "ls", BASE_URL + tableName + "/"],
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        (sout, serr) = sc.communicate()
        bucket = sout.decode("utf-8").split('\n')
        bucket.remove('')
        bucket = [b.replace(BASE_URL + tableName + '/', '') for b in bucket]
        print("Bucket has the following file(s):", bucket)
    except FileNotFoundError:
        print(
            f"{EXEC} is not installed on your system. Cannot proceed with the downloading.")
        return -2

    # Downloaded files counter
    count = 0

    # Open destination file
    with open(LOCALDATA_PATH + tableName + ".csv", 'w') as mergedFile:
        # For each file in the bucket
        for b in bucket:
            if (count < nbFilesMax):
                count += 1
                try:
                    # Download it
                    subprocess.call([EXEC, "cp", BASE_URL + tableName + "/" + b,
                                    TEMP_PATH], stderr=subprocess.PIPE, stdout=subprocess.PIPE)              
                    # Open and copy to destination file
                    with gzip.open(TEMP_PATH + b, 'rt') as file:
                        mergedFile.writelines(file.readlines())
                        print(f"Written lines from {b} in {tableName}. ", end="")
                    # Delete the temp file
                    os.remove(TEMP_PATH + b)
                    print(f"Removed file {TEMP_PATH}{b}")
                except FileNotFoundError:
                    print("This is bad and shouldn't happen at all.")
                    return -4

    print(f"Over. datadl.py has gathered {count} file(s) in total for this analysis.")

    # remove temp folder
    try:
        shutil.rmtree(TEMP_PATH)
    except OSError as e:
        print(f"Error while deleting {e.filename} -> {e.strerror}.")
        return -3

    print(f"Successfully gathered table {tableName} as a csv file.")
    return 0


if __name__ == "__main__":
    print(f"This file is not conceived to be executed directly.")
    download("task_events")
