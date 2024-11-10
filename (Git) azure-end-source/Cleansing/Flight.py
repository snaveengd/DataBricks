# Databricks notebook source
# MAGIC %md
# MAGIC # Alternative approach

# COMMAND ----------

# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Utilities/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

PLANE_path = "/mnt/raw_datalake/flight/"
df = spark.read.format("csv").options(header =
True).load(PLANE_path)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/flight")

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/flight")

# COMMAND ----------

# MAGIC %sql
# MAGIC    select * from cleansed_EndtoEnd.flight

# COMMAND ----------

# MAGIC %md
# MAGIC # Alternative approach end

# COMMAND ----------

PLANE_path = "/mnt/raw_datalake/Airport/"
df = spark.read.format("csv").options(header =
True).load(PLANE_path)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

import pandas as pd
PLANE_path = "/mnt/raw_datalake/PLANE/"

df = pd.read_csv(PLANE_path)

print(df.to_string()) 

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SpaceSeparatedCSV").getOrCreate()

# Define the path to your CSV file
csv_file_path = '/mnt/raw_datalake/PLANE/'

# Read the CSV file with space as the delimiter
df = spark.read.option("delimiter", " ").option("header", "true").csv(csv_file_path)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Initialize Spark session
spark = SparkSession.builder.appName("SpaceSeparatedCSV").getOrCreate()

# Define the path to your CSV file
csv_file_path = '/mnt/raw_datalake/PLANE/'

# Read the CSV file as text
lines = spark.read.text(csv_file_path)

# Split each line by whitespace
# Adjust the split to match your actual data format
df = lines.select(
    split(col("value"), "\s+").alias("values")
).select(
    col("values").getItem(0).alias("tailnum"),
    col("values").getItem(1).alias("type"),
    col("values").getItem(2).alias("manufacturer"),
    col("values").getItem(3).alias("issue_date"),
    col("values").getItem(4).alias("model"),
    col("values").getItem(5).alias("status"),
    col("values").getItem(6).alias("aircraft_type"),
    col("values").getItem(7).alias("engine_type"),
    col("values").getItem(8).alias("year")
)

# Show the DataFrame
df.show()

