BUCKET='wallbucket'
JOB=$1

function creater_cluster () {
    gcloud dataproc clusters create pysparking \
    --enable-component-gateway \
    --bucket $1 \
    --region europe-west1 \
    --subnet default \
    --zone europe-west1-b \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 50 \
    --num-workers $2 \
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
    --files=job/job.py job/job${JOB}_${IMP}.py -- ${JOB_NAME}"
}

for nb_nodes in {2,4,8}
    do
        JOB_NAME="job_${JOB}_${nb_nodes}nodes"
        echo "Creating cluster for job [${JOB_NAME}]"
        creater_cluster $BUCKET $nb_nodes
        
        echo "Lauching job [${JOB_NAME}]"
        rdd="$(launch_job $JOB $JOB_NAME 'rdd')"
        df="$(launch_job $JOB $JOB_NAME 'df')"
        echo -e "\t rdd version"
        $($rdd)
        echo -e "\t df version"
        $($df)

        gcloud dataproc clusters delete pysparking --region=europe-west1 --quiet
    done
