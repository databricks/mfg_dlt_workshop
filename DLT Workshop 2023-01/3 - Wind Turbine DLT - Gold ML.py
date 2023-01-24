# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Building our Data Pipeline  Delta Live Tables
# MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
# MAGIC 
# MAGIC Ingesting data in streaming and applying ML on top of it can be a real challenge. <br/>
# MAGIC Delta Live Table and the Lakehouse simplify Data Engineering by handling all the complexity for you, while you can focus on your business transformation.
# MAGIC 
# MAGIC **Accelerate ETL development** <br/>
# MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
# MAGIC 
# MAGIC **Remove operational complexity** <br/>
# MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
# MAGIC 
# MAGIC **Trust your data** <br/>
# MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
# MAGIC 
# MAGIC **Simplify batch and streaming** <br/>
# MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
# MAGIC 
# MAGIC ### Building a Delta Live Table pipeline for Predictive maintenance
# MAGIC 
# MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our sensor data and making inferences.
# MAGIC 
# MAGIC - We'll first create a dataset that we'll use to train our ML Model.
# MAGIC - Once the model is trained and deployed, we'll simply call it in our Delta Live Table pipeline to run inferences at scale, and in real time.
# MAGIC - Ultimately, we'll use this data to build our tracking Dashboard with DBSQL and track our Wind Turbine health.
# MAGIC 
# MAGIC 
# MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-0.png" width="1000px"/></div>
# MAGIC 
# MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c72782ae-982a-4308-8d00-93dcf36a3519/updates/2d250b66-42fc-43c9-98df-d67653b00f12) to see this flow in action
# MAGIC 
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_ingestion_sql&dt=MANUFACTURING_WIND_TURBINE">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## 4. Load model from MLFlow registry and apply inference
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-5.png" width="600px" style="float: right"/>
# MAGIC 
# MAGIC 
# MAGIC Our DataScientist team built a ML model using the training dataset that we deployed.
# MAGIC 
# MAGIC They used Databricks AutoML to bootstrap their project and accelerate the time to deployment. The model has been packaged in Databricks Model registry and we can now easily load it within our DLT pipeline.
# MAGIC 
# MAGIC For more detail on the model creation steps and how Databricks can accelerate Data Scientists, open the model training notebook (`02-Wind Turbine SparkML Predictive Maintainance`)
# MAGIC 
# MAGIC As you can see, loading the model is a simple call to our Model Registry. 
# MAGIC 
# MAGIC We don't need to know how the model designed by our Data Scientist team is working, Databricks take care of that for you.
# MAGIC 
# MAGIC *Note: for this demo we create only 1 pipeline for both training and inference, a production-grade project will likely have 2 pipeline: 1 for training the dataset and 1 for the inference.*

# COMMAND ----------

# DBTITLE 1,Load Model from registry and register it as SQL function
import mlflow
get_turbine_status = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_wind_turbine_maintenance/Production", "string")

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

@dlt.table
def turbine_gold_dlt():
  return (
    dlt.read_stream("sensors_silver_dlt").select(get_turbine_status(struct(col("AN3"), col("AN4"), col("AN5"), col("AN6"), col("AN7"), col("AN8"), col("AN9"), col("AN10"))).alias("status_prediction"),"sensors_silver_dlt.*")
  )

# COMMAND ----------

# DBTITLE 1,Let's request our final gold table containing the predictions as example
# MAGIC %sql 
# MAGIC select * from field_demos_streaming.turbine_gold_dlt

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC Our [DLT Data Pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/085d6ea8-e1c9-4058-94fc-05ea960a2421/updates/9afd62f7-3674-4ae2-99d0-f42c253d0b7f) is now ready using purely SQL. We have an end 2 end cycle, and our ML model has been integrated seamlessly by our Data Engineering team.
# MAGIC 
# MAGIC 
# MAGIC For more details on model training, open the [model training notebook]($./02-Wind Turbine SparkML Predictive Maintenance)
# MAGIC 
# MAGIC Our final dataset includes our ML prediction for our Predictive Maintenance use-case. 
# MAGIC 
# MAGIC We are now ready to build our [DBSQL Dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/048c6d42-ad56-4667-ada1-e35f80164248-turbine-demo-predictions?o=1444828305810485) to track the main KPIs and status of our entire Wind Turbine Farm. 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/wind-turbine-dashboard.png" width="1000px">
