# Databricks notebook source
kafka_bootstrap_servers_tls = dbutils.secrets.get("streaming-demo","kafka_bootstrap_tls")
kafka_bootstrap_servers_plaintext = dbutils.secrets.get("streaming-demo","kafka_bootstrap_plaintext")

# COMMAND ----------

# Full username, e.g. "<first>.<last>@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_wind_turbine_dlt_streaming"

checkpoint_location = f"{project_dir}/kafka_checkpoint"

topic = f"wind_turbine_dlt_streaming_8"

# COMMAND ----------

print(f"Topic Name:   {topic}")

# COMMAND ----------

# DBTITLE 1,Read Turbine Dataset (200k records)
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid

addmonths = 32

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

input_path = "/mnt/field-demos/streaming/iot_turbine/incoming-data-json"
input_schema = spark.read.json(input_path).schema

input_stream = (spark
  .readStream
  .schema(input_schema)
  .json(input_path)
  .withColumn("TIMESTAMP", to_timestamp(col('TIMESTAMP'),"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")+expr("INTERVAL "+str(addmonths)+" MONTHS"))
  .withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp"))
  .withColumn("eventId", uuidUdf()))

# COMMAND ----------

# DBTITLE 1,Publish to Kafka Topic (new records)
# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"

# also must cast the string timestamp in format 2020-05-18T19:04:37.000Z to a timestamp

(input_stream
   .select(col("eventId").alias("key"), to_json(struct(col('TORQUE'), col('TIMESTAMP'),col('SPEED'),col('ID'), col('AN3'), col('AN4'), col('AN5'),col('AN6'),col('AN7'),col('AN8'),col('AN9'),col('AN10'),col('processingTime'))).alias("value"))
   .writeStream
   .format("kafka")
   .trigger(once=True) \
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
   .option("kafka.security.protocol", "SSL")
   .option("checkpointLocation", checkpoint_location )
   .option("topic", topic)
   .start()
)

# COMMAND ----------

# DBTITLE 0,Consume & Test Kafka Source 
# startingOffsets = "earliest" 

# # In contrast to the Kafka write in the previous cell, when we read from Kafka we use the unencrypted endpoints.
# # Thus, we omit the kafka.security.protocol property
# kafka = (spark.readStream
#   .format("kafka")
#   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
#   .option("subscribe", topic )
#   .option("startingOffsets", startingOffsets )
#   .load())

# read_stream = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json"))

# display(read_stream)
