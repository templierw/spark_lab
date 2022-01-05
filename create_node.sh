gcloud dataproc clusters create singlenode \
    --enable-component-gateway \
    --bucket wallbucket \
    --region europe-west1 --zone europe-west1-b \
    --subnet default --single-node \
    --master-machine-type n1-standard-8 --master-boot-disk-size 100 \
    --image-version 2.0-ubuntu18 --optional-components JUPYTER \
    --project lsdm-pyspark \
    --initialization-actions 'gs://wallbucket/init_node.sh'