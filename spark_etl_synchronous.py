from pyspark.sql import SparkSession

database = ""
db_url = f"jdbc:sqlserver://ipaddresshere;databaseName={database};encrypt=true;trustServerCertificate=true;"
db_user = ""
db_password = "P"

def read_config(table):

    config_df = (
        spark.read
        .format("bigquery")
        .option("table", table)
        .load()
    )

    config = config_df.collect()

    for row in config:
        load_table(row["sourceTableName"], row["targetTableName"])

def load_table(source, target):
    print(f'source: {source}')
    print(f'target: {target}')

    load_df = (
        spark.read
        .format("jdbc")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")   
        .option("url", db_url)
        .option("dbtable", source)
        .option("user", db_user)
        .option("password", db_password)
        .load()
    )

    load_df.write \
    .format('bigquery') \
    .mode("overwrite") \
    .option("writeMethod", "direct") \
    .save(target)        

spark = SparkSession.builder.getOrCreate()

read_config("adventureworks_raw.elt_config")