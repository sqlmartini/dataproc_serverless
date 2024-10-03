from pyspark.sql import SparkSession
from threading import Thread
from queue import Queue

# Source variables
database = ""
source_ip = "10.2.0.2:1433"
db_url = f"jdbc:sqlserver://{source_ip};databaseName={database};encrypt=true;trustServerCertificate=true;"
db_user = ""
db_password = ""

def read_config(table):

    config_df = (
        spark.read
        .format("bigquery")
        .option("table", table)
        .load()
    )

    config = config_df.collect()
    return config

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

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        function(value[0], value[1])
        q.task_done()    

spark = SparkSession.builder.getOrCreate()

config = read_config("adventureworks_raw.elt_config") #change to your control dataset.table

q = Queue()
worker_count = 30

for row in config:
    q.put([row["sourceTableName"], row["targetTableName"]])

for i in range(worker_count):
    t=Thread(target=run_tasks, args=(load_table, q))
    t.daemon = True
    t.start()

q.join()