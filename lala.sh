awk 'BEGIN {
    cmd = "gsutil ls gs://clusterdata-2011-2/task_usage/"
    while ( ( cmd | getline result ) > 0 ) {
        split(result, array1, "//*");
        file=array1[4];
        cmd2="gsutil cp gs://clusterdata-2011-2/task_usage/"file" ./";
        system(cmd2)
    }
    close(cmd);
}'