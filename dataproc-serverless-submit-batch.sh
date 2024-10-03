PROJECT_ID=""
REGION=""
JAR_FILE=""
JOB_FILE=""
SUBNET=""
SERVICE_ACCOUNT=""

gcloud dataproc batches submit --project $PROJECT_ID \
    --region $REGION \
    pyspark $JOB_FILE \
    --version 2.2 \
    --jars $JAR_FILE \
    --service-account $SERVICE_ACCOUNT \
    --subnet $SUBNET