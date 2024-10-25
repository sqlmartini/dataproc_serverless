from pyspark.sql import SparkSession
from google.cloud import secretmanager
import sys

def access_secret(project_id, secret_id, version_id="latest"):
    """
    Access the payload for the given secret
    """

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    payload = response.payload.data.decode("UTF-8")
    return payload

def read_config(table, url, user, password):
    """
    Read the ETL source to target mapping configuration
    """    
    
    config_df = (
        spark.read
        .format("jdbc")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")   
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()
    )

    config = config_df.collect()
    return config

def extract(table, url, user, password, partitionColumn, lowerBound, upperBound, numPartitions):
    """
    Read from source and return a dataframe
    """    
    
    print(f'source: {source}')

    extract_df = (
        spark.read
        .format("jdbc")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")   
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("partitionColumn", partitionColumn)
        .option("lowerBound", lowerBound)
        .option("upperBound", upperBound)
        .option("numPartitions", numPartitions)        
        .load()
    )

    return extract_df

def load(df, target):
    """
    Write to target table
    """   
    
    print(f'target: {target}')

    (df.write
        .format('bigquery')
        .mode("overwrite")
        .option("table", target)
        .option("writeMethod", "direct")
        .save())

if __name__ == "__main__":
        
    spark = SparkSession.builder.getOrCreate()

    # Parse arguments
    project_id = sys.argv[1]

    # Retrieve secret manager secrets
    user = access_secret(project_id, "airflow-variables-cloudsql-username")
    password = access_secret(project_id, "airflow-variables-cloudsql-password")
    ip = access_secret(project_id, "airflow-variables-cloudsql-ip")

    # Source variables
    database = "AdventureWorks2022"
    url = f"jdbc:sqlserver://{ip};databaseName={database};encrypt=true;trustServerCertificate=true;"
    config_table = "adventureworks_raw.elt_config"

    # Read ETL configuration
    config = read_config(config_table, url, user, password)    

    # Loop through ETL configuration, read source and load to target
    for row in config:    
        df = extract(row["sourceTableName"], url, user, password, row["partitionColumn"], row["lowerBound"], row["upperBound"], row["numPartitions"])
        load(df, row["targetTableName"])