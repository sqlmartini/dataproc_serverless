PROJECT_ID=""
REGION=""
JAR_FILE=""
JOB_FILE=""
SUBNET=""
SERVICE_ACCOUNT=""

PROJECT_ID="amm-beacon-demo"
REGION="us-central1"
JAR_FILE="gs://s8s_data_and_code_bucket-188308391391/drivers/mssql-jdbc-12.4.0.jre8.jar"
JOB_FILE="gs://s8s_data_and_code_bucket-188308391391/scripts/pyspark/test.py"
SUBNET="spark-snet"
SERVICE_ACCOUNT="cdf-lab-sa@amm-beacon-demo.iam.gserviceaccount.com"

gcloud dataproc batches submit --project $PROJECT_ID \
    --region $REGION \
    pyspark $JOB_FILE \
    --version 2.2 \
    --jars $JAR_FILE \
    --service-account $SERVICE_ACCOUNT \
    --subnet $SUBNET

gcloud dataproc batches submit --project becn-data-warehouse \
    --region us-east4 pyspark \
    --batch sqldwmigration4 gs://dataproc-poc-bucket/pysparkcode/sqltablemigration.py \
    --version 2.2 \
    --jars gs://dataproc-poc-bucket/mssql-jdbc-12.8.1.jre8.jar \
    --subnet projects/shared-vpc-prod-1/regions/us-east4/subnetworks/dev-sql-vpc-vm-subnet \
    --properties spark.dataproc.appContext.enabled=true    