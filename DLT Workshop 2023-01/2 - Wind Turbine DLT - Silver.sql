-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Wind Turbine Predictive Maintenance with the Lakehouse
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="500px" style="float:right; margin-left: 20px"/>
-- MAGIC Predictive maintenance is a key capabilities in Manufacturing industry. Being able to repair before an actual failure can drastically increase productivity and efficiency, preventing from long and costly outage. <br/> 
-- MAGIC 
-- MAGIC Typical use-cases include:
-- MAGIC 
-- MAGIC - Predict valve failure in gaz/petrol pipeline to prevent from industrial disaster
-- MAGIC - Detect abnormal behavior in a production line to limit and prevent manufacturing defect in the product
-- MAGIC - Repairing early before larger failure leading to more expensive reparation cost and potential product outage
-- MAGIC 
-- MAGIC In this demo, our business analyst have determined that if we can proactively identify and repair Wind turbines prior to failure, this could increase energy production by 20%.
-- MAGIC 
-- MAGIC In addition, the business requested a predictive dashboard that would allow their Turbine Maintenance group to monitore the turbines and identify the faulty one. This will also allow us to track our ROI and ensure we reach this extra 20% productivity gain over the year.
-- MAGIC 
-- MAGIC 
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_dlt&dt=MANUFACTURING_WIND_TURBINE">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Building our Data Pipeline  Delta Live Tables
-- MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC 
-- MAGIC Ingesting data in streaming and applying ML on top of it can be a real challenge. <br/>
-- MAGIC Delta Live Table and the Lakehouse simplify Data Engineering by handling all the complexity for you, while you can focus on your business transformation.
-- MAGIC 
-- MAGIC **Accelerate ETL development** <br/>
-- MAGIC Enable analysts and data engineers to innovate rapidly with simple pipeline development and maintenance 
-- MAGIC 
-- MAGIC **Remove operational complexity** <br/>
-- MAGIC By automating complex administrative tasks and gaining broader visibility into pipeline operations
-- MAGIC 
-- MAGIC **Trust your data** <br/>
-- MAGIC With built-in quality controls and quality monitoring to ensure accurate and useful BI, Data Science, and ML 
-- MAGIC 
-- MAGIC **Simplify batch and streaming** <br/>
-- MAGIC With self-optimization and auto-scaling data pipelines for batch or streaming processing 
-- MAGIC 
-- MAGIC ### Building a Delta Live Table pipeline for Predictive maintenance
-- MAGIC 
-- MAGIC In this example, we'll implement a end 2 end DLT pipeline consuming our sensor data and making inferences.
-- MAGIC 
-- MAGIC - We'll first create a dataset that we'll use to train our ML Model.
-- MAGIC - Once the model is trained and deployed, we'll simply call it in our Delta Live Table pipeline to run inferences at scale, and in real time.
-- MAGIC - Ultimately, we'll use this data to build our tracking Dashboard with DBSQL and track our Wind Turbine health.
-- MAGIC 
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-0.png" width="1000px"/></div>
-- MAGIC 
-- MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/c72782ae-982a-4308-8d00-93dcf36a3519/updates/2d250b66-42fc-43c9-98df-d67653b00f12) to see this flow in action
-- MAGIC 
-- MAGIC 
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fmanufacturing%2Fwind_turbine%2Fnotebook_ingestion_sql&dt=MANUFACTURING_WIND_TURBINE">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 2. Silver layer: clean bronze sensors, power data and read in status
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-2.png" width="600px" style="float: right"/></div>
-- MAGIC 
-- MAGIC Let's cleanup a little bit the bronze sensor.
-- MAGIC 
-- MAGIC We'll use DLT Expectations to enforce Data Quality.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sensors_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0 and id < 500) ON VIOLATION DROP ROW
)
COMMENT "Cleaned data for analysis."
AS SELECT AN3, AN4, AN5, AN6, AN7, AN8, AN9, AN10, ID, SPEED, TIMESTAMP
  from STREAM(live.sensors_bronze_dlt)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 3. Build the Training Dataset for the DS team
-- MAGIC 
-- MAGIC <div><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-flow-3.png" width="600px" style="float: right"/></div>
-- MAGIC 
-- MAGIC As next step, we'll join the sensor data with our labeled dataset to know which Turbine was damaged.
-- MAGIC 
-- MAGIC This table will then be used by the Data Science team to build our ML model detecting damaged turbine.

-- COMMAND ----------

CREATE LIVE TABLE status_silver_dlt (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL and id > 0 and id < 500) ON VIOLATION DROP ROW
)
  COMMENT "Turbine status"
AS SELECT * FROM parquet.`/mnt/field-demos/streaming/iot_turbine/status`

-- COMMAND ----------

CREATE STREAMING LIVE TABLE training_dataset_sensor_dlt 
  COMMENT "Final sensor table with all information for Analysis / ML"
AS SELECT * FROM STREAM(live.sensors_silver_dlt) LEFT JOIN live.status_silver_dlt USING (id)
