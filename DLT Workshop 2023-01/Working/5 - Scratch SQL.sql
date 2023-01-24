-- Databricks notebook source
select extract(hour from timestamp), count(distinct id) from hive_metastore.dlt_workshop_mfg.sensors_bronze_dlt
group by 1

select extract(hour from timestamp), count(distinct id) from hive_metastore.dlt_workshop_mfg.sensors_bronze_dlt
group by 1

select * from json(

select current_database()

use dlt_workshop_mfg


select * from cloud_files("/mnt/field-demos/streaming/iot_turbine/incoming-data-json", "json")

select * from jsonTable

select * from blah

select * from status_silver_dlt

select * from turbine_gold_dlt

-- COMMAND ----------

CREATE TEMPORARY VIEW jsonTable
USING org.apache.spark.sql.json
OPTIONS (
  path "/mnt/field-demos/streaming/iot_turbine/incoming-data-json"
)


-- COMMAND ----------

select extract(month from timestamp), count(distinct id), min(id), max(id) from hive_metastore.dlt_workshop_mfg.sensors_silver_dlt
group by 1
