# Databricks notebook source
# MAGIC %md
# MAGIC # Alternative approach

# COMMAND ----------

# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Cleansing/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

Cancellation_path = "/mnt/raw_datalake/Cancellation/"
df = spark.read.parquet(Cancellation_path)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Remove double quotes from all string columns
df_base = df.select(
    regexp_replace(col("Code"), '"', '').alias("Code"),
    regexp_replace(col("Description"), '"', '').alias("Description"),
    to_date(col("Date_Part"),'yyyy-MM-dd').alias("Date_Part")
)

# COMMAND ----------

display(df_base)

# COMMAND ----------

df_base.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/Cancellation")

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/Cancellation")
schema=pre_schema(df_Plane_read)
f_delta_cleansed_load(schema,'/mnt/cleansed_datalake/Cancellation','Cancellation','cleansed_EndtoEnd')	

# COMMAND ----------

# MAGIC %sql
# MAGIC    select * from cleansed_EndtoEnd.Cancellation

# COMMAND ----------

# MAGIC %md
# MAGIC # Alternative approach end
