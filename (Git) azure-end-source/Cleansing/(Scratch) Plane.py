# Databricks notebook source
df=spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
          .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
            .load('/mnt/raw_datalake/PLANE')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import input_file_name

# Define the path to the mounted directory
input_path = "/mnt/raw_datalake/PLANE"

# Use Auto Loader to read the files
df_airlines = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/PLANE") \
    .option("cloudFiles.useNotifications", "true") \
    .load(input_path)

# Add a column to display the file path
df_with_file_path = df_airlines.withColumn("file_path", input_file_name())

# # Write the stream to the console for debugging purposes
# query = df_with_file_path.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "/dbfs/FileStore/tables/checkpoints/airlines") \
#     .start()

# query.awaitTermination()


# COMMAND ----------

display(df_airlines)

# COMMAND ----------

airlines

# COMMAND ----------

df_airlines=spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
          .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/flight")\
            .load('/mnt/raw_datalake/flight')

# COMMAND ----------

display(df_airlines)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/PLANE/Date_Part=2024-11-08/AIRPORT.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Alternative approach

# COMMAND ----------

# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Cleansing/Utilities
# MAGIC
# MAGIC

# COMMAND ----------

PLANE_path = "/mnt/raw_datalake/PLANE/"
df = spark.read.format("csv").options(header =
True).load(PLANE_path)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df.select(to_date(col("Date_Part"), "yyyy-MM-dd").alias("date"))

# COMMAND ----------

df_base=df.select(col("tailnum").alias("tailid"),col("type"),col("manufacturer"),to_date(col("issue_date")).alias("issue_date"),col("model"),col("status"),col("aircraft_type"),col("engine_type"),col("year").cast("int"),to_date(col("Date_Part"), "yyyy-MM-dd").alias("date"))


# COMMAND ----------

display(df_base)

# COMMAND ----------

df_base.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/Plane")

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/Plane")

# COMMAND ----------

l=df_Plane_read.dtypes

# COMMAND ----------

print(l)

# COMMAND ----------

for i in l:
   print(i[0]+' '+i[1]+',')

# COMMAND ----------

schema=''
for i in l:
  schema= schema+i[0]+' '+i[1]+','
print(schema[0:-1]) # to remove , at the end

# COMMAND ----------

def pre_schema(df):
  try:
    schema=''
    for i in df.dtypes:
        schema= schema+i[0]+' '+i[1]+','
    return schema[0:-1] # to remove , at the end
  except Exception as err:
    print("Error occured",str(err))

# COMMAND ----------

# def f_delta_cleansed_load(schema,location,table_name,database)):
#   try:
#       spark.sql(""" 
#                 CREATE TABLE IF NOT EXISTS {3}.{2}
#                 (
#                 {0} 
#                 )
#                 using delta
#                 location {1}
#                 """.format(schema,location,table_name,database))
#   except Exception as err:
#     print("Error Occure",str(err))   

# COMMAND ----------

def f_delta_cleansed_load(schema, location, table_name, database):
    try:
        spark.sql(""" 
            CREATE TABLE IF NOT EXISTS {database}.{table_name}
            (
            {schema} 
            )
            USING delta
            LOCATION '{location}'
        """.format(schema=schema, location=location, table_name=table_name, database=database))
    except Exception as err:
        print("Error Occurred:", str(err))

# COMMAND ----------

df_Plane_read=spark.read.format("delta").load("/mnt/cleansed_datalake/Plane")
schema=pre_schema(df_Plane_read)
f_delta_cleansed_load(schema,'/mnt/cleansed_datalake/Plane','Plane','cleansed_EndtoEnd')

# COMMAND ----------

# MAGIC %sql
# MAGIC    select * from cleansed_EndtoEnd.Plane

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

