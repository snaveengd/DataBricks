# Databricks notebook source
# MAGIC %md
# MAGIC # Alternative approach

# COMMAND ----------

# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Cleansing/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

PLANE_path = "/mnt/raw_datalake/Airport/"
df = spark.read.format("csv").options(header =
True).load(PLANE_path)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_base = df.select(
    col("Code"),
    split(col("Description"), ',').getItem(0).alias("City"), split(split(col("Description"), ',').getItem(1),':').getItem(0).alias("Country"),
    split(split(col("Description"), ',').getItem(1),':').getItem(1).alias("Airport"), to_date(col("Date_Part"),'yyyy-MM-dd').alias("Data_part")
)

# COMMAND ----------

display(df_base)

# COMMAND ----------

df_base.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/Airport")

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/Airport")
schema=pre_schema(df_read)
f_delta_cleansed_load(schema,'/mnt/cleansed_datalake/Airport','Airport','cleansed_EndtoEnd')	

# COMMAND ----------

# MAGIC %sql
# MAGIC    select * from cleansed_EndtoEnd.Airport

# COMMAND ----------

# MAGIC %md
# MAGIC # Alternative approach end
