# Databricks notebook source
# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Cleansing/Utilities

# COMMAND ----------

df=spark.read.json("/mnt/raw_datalake/airlines/")

# COMMAND ----------

display(df)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df1=df.select(explode("response"),"Date_Part")

# COMMAND ----------

display(df1)

# COMMAND ----------

df_final=df1.select("col.*","Date_Part")

# COMMAND ----------

display(df_final)

# COMMAND ----------

dbutils.fs.rm('/mnt/cleansed/airline',True)

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

dbutils.fs.ls("/mnt/cleansed_datalake/")

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/airline")
schema=pre_schema(df_Plane_read)
f_delta_cleansed_load(schema,'/mnt/cleansed_datalake/airline','airline','cleansed_EndtoEnd')	

# COMMAND ----------

# MAGIC %sql
# MAGIC    select * from cleansed_EndtoEnd.airline
