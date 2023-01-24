# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Wind Turbine Predictive Maintenance with the Lakehouse
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="500px" style="float:right; margin-left: 20px"/>
# MAGIC Predictive maintenance is a key capabilities in Manufacturing industry. Being able to repair before an actual failure can drastically increase productivity and efficiency, preventing from long and costly outage. <br/> 
# MAGIC 
# MAGIC Typical use-cases include:
# MAGIC 
# MAGIC - Predict valve failure in gaz/petrol pipeline to prevent from industrial disaster
# MAGIC - Detect abnormal behavior in a production line to limit and prevent manufacturing defect in the product
# MAGIC - Repairing early before larger failure leading to more expensive reparation cost and potential product outage
# MAGIC 
# MAGIC In this demo, our business analyst have determined that if we can proactively identify and repair Wind turbines prior to failure, this could increase energy production by 20%.
# MAGIC 
# MAGIC In addition, the business requested a predictive dashboard that would allow their Turbine Maintenance group to monitore the turbines and identify the faulty one. This will also allow us to track our ROI and ensure we reach this extra 20% productivity gain over the year.
# MAGIC 
# MAGIC 
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_dlt&dt=MANUFACTURING_WIND_TURBINE">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Load model from MLFlow registry and apply inference to a Live Stream - Alert on Predictions
# MAGIC 
# MAGIC 
# MAGIC Now that we have an ML Model that our Data Scientists have built, let's use it to do real-time inference on streaming sensor data.  Any positive predictions can be written to an alert consumer, which can then be configured to trigger pager alerts.

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

kafka_bootstrap_servers_tls = dbutils.secrets.get("streaming-demo","kafka_bootstrap_tls")
kafka_bootstrap_servers_plaintext = dbutils.secrets.get("streaming-demo","kafka_bootstrap_plaintext")

# COMMAND ----------

# Full username, e.g. "<first>.<last>@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/kafka_wind_turbine_dlt_streaming"

checkpoint_location = f"{project_dir}/rt/kafka_checkpoint"

source_topic = f"{user}_wind_turbine_dlt_streaming"
target_topic = f"{user}_wind_turbine_dlt_streaming_alerts"

# COMMAND ----------

input_path = "/mnt/field-demos/streaming/iot_turbine/incoming-data-json"
input_schema = spark.read.json(input_path).schema

# COMMAND ----------

from pyspark.sql.functions import from_json, col, struct

startingOffsets = "earliest"

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", source_topic )
  .option("startingOffsets", startingOffsets )
  .load())

# COMMAND ----------

# DBTITLE 1,Load Model from MLflow and Register as a function
import mlflow
get_turbine_status = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_wind_turbine_maintenance/Production", "string")

# COMMAND ----------

predictions = (kafka.select(from_json(col("value").cast("string"), input_schema).alias("json"))
                    .select('json.*')
                    .select(col("ID"),
                       get_turbine_status(struct(col("AN3"), col("AN4"), col("AN5"), col("AN6"), col("AN7"), col("AN8"), col("AN9"), col("AN10")))
                      .alias("status_prediction"))
              )

# COMMAND ----------

from pyspark.sql.functions import *

# Clear checkpoint location
dbutils.fs.rm(checkpoint_location, True)

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"
(predictions
   .filter("status_prediction > 0")
   .select(to_json(struct(col('ID'), col('status_prediction'))).alias("value"))
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
   .option("kafka.security.protocol", "SSL")
   .option("checkpointLocation", checkpoint_location )
   .option("topic", target_topic)
   .start()
)

# COMMAND ----------


