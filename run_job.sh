BUCKET='wallbucket'

function creater_cluster () {
    gcloud dataproc clusters create pysparking \
    --enable-component-gateway \
    --bucket $1 \
    --region europe-west1 \
    --subnet default \
    --zone europe-west1-b \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 50 \
    --num-workers 5 \
    --worker-machine-type n1-highmem-4 \
    --worker-boot-disk-size 50 \
    --image-version 2.0-ubuntu18 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project lsdm-pyspark
}

function launch_job () {
    JOB=$1
    JOB_NAME="$2_$3"
    IMP=$3
    echo "gcloud dataproc jobs submit pyspark \
    --cluster=pysparking --region=europe-west1 \
    --files=lib.py job/job${JOB}_${IMP}.py -- ${JOB_NAME}"
}

repeat(){
	for i in $( seq 1 $1 ); do echo -n "#"; done
    echo
}

echo "Creating cluster..."
creater_cluster $BUCKET 

for job in {1,2,3,4,5}; do
        JOB_NAME="job_${job}"
        n="Lauching job [${JOB_NAME}]"
        repeat ${#n}
        echo $n
        repeat ${#n}

        echo -e "\n\t ### rdd version ###\n"
        $(launch_job $job $JOB_NAME 'rdd')
        echo -e "\n\t ### df version ###\n"
        $(launch_job $job $JOB_NAME 'df')
done

gcloud dataproc clusters delete pysparking --region=europe-west1 --quiet
