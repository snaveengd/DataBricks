# Databricks notebook source
pip install tabula-py

# COMMAND ----------

from datetime import date
print(date.today())

# COMMAND ----------

dbutils.fs.ls('mnt/source_blob/')


# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/source_blob/PLANE.pdf')

# COMMAND ----------

dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

# successfully

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_dir = f'/mnt/raw_datalake/PLAIN/Date_Part={today}'

# Use dbutils.fs.mkdirs for creating directories
dbutils.fs.mkdirs(output_dir)

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_csv_path = f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={today}/PLAIN.csv'

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pdf_df)

    # Write the Spark DataFrame to CSV
    spark_df.write.mode("overwrite").option("header", "true").csv(output_dir)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_dir = f'/mnt/raw_datalake/PLAIN/Date_Part={today}'

# Use dbutils.fs.mkdirs for creating directories
dbutils.fs.mkdirs(output_dir)

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_csv_path = f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={today}/PLAIN.csv'

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove extra headers if present
    pdf_df = pdf_df.loc[:,~pdf_df.columns.duplicated()]

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pdf_df)

    # Write the Spark DataFrame to a single CSV file
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_dir = f'/mnt/raw_datalake/PLAIN/Date_Part={today}'

# Use dbutils.fs.mkdirs for creating directories
dbutils.fs.mkdirs(output_dir)

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove extra headers if present
    pdf_df = pdf_df.loc[:, ~pdf_df.columns.duplicated()]

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pdf_df)

    # Write the Spark DataFrame to a single CSV file
    temp_output_dir = f"{output_dir}/temp"
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_dir)

    # Rename the single CSV file to the desired name
    temp_csv_path = os.path.join(temp_output_dir, os.listdir(temp_output_dir)[0])
    dbutils.fs.mv(temp_csv_path, output_csv_path)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------


#failed
import tabula
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
today = str(date.today())
output_dir = f'/mnt/raw_datalake/PLAIN/Date_Part={today}'

# Use dbutils.fs.mkdirs for creating directories
dbutils.fs.mkdirs(output_dir)

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Define the correct headers
    correct_headers = [
        "tailnum", "type", "manufacturer", "issue_date", "model", "status",
        "aircraft_type", "engine_type", "year"
    ]
    
    # Filter out rows that match header row
    pdf_df = pdf_df[~pdf_df.apply(lambda x: x.str.contains("tailnum|type|manufacturer|issue_date|model|status|aircraft_type|engine_type|year").any(), axis=1)]

    # Assign correct headers to the DataFrame
    pdf_df.columns = correct_headers

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pdf_df)

    # Write the Spark DataFrame to a single CSV file
    temp_output_dir = f"{output_dir}/temp"
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_dir)

    # Rename the single CSV file to the desired name
    temp_csv_path = os.path.join(temp_output_dir, os.listdir(temp_output_dir)[0])
    dbutils.fs.mv(temp_csv_path, output_csv_path)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

import tabula
from datetime import date
tabula.convert_into('/dbfs/mnt/source_blob/PLANE.pdf',f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={date.today()}/PLAIN.csv',output_format='csv',pages='all')


# COMMAND ----------

import tabula
from datetime import date
print(date.today())
today=str(date.today())
output_dir = f'/dbfs/mnt/raw_datalake/PLAlN/Datepart={today}'
dbutils.fs.mkdirs(output_dir)
tabula.convert_into(
'/dbfs/mnt/source_blob/PLANE.pdf',
f'{output_dir}'+"/PLAIN.csv",
output_format='csv',
pages='all'
)

# COMMAND ----------

from datetime import date

print(date.today())
today = str(date.today())
output_dir = f"/dbfs/mnt/raw_datalake/PLAlN/Datepart={today}"
dbutils.fs.mkdirs(output_dir)

tabula.convert_into(
    "/dbfs/mnt/source_blob/PLANE.pdf",
    f"{output_dir}/PLAIN.csv",
    output_format="csv",
    pages="all"
)

# COMMAND ----------

import tabula
from datetime import date
tabula.convert_into('/dbfs/mnt/source_blob/PLANE.pdf','/dbfs/mnt/raw_datalake/PLAIN/PLAIN.csv',output_format='csv',pages='all')

# COMMAND ----------



import tabula
import pandas as pd
import os

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_csv_path = '/dbfs/mnt/raw_datalake/PLAIN/PLAIN.csv'

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(output_csv_path, index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd
import os
import shutil

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_dir = '/dbfs/mnt/raw_datalake/PLAIN'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(output_csv_path, index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the source PDF path and output CSV path
source_pdf_path = 'dbfs:/mnt/source_blob/PLANE.pdf'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(f'/dbfs{output_csv_path}', index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the source PDF path and output CSV path
source_pdf_path = 'dbfs:/mnt/source_blob/PLANE.pdf'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(f'/dbfs{output_csv_path}', index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# List the contents of the source directory
display(dbutils.fs.ls('/mnt/source_blob/'))


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Define the source PDF path and output CSV path
source_pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_path = f'{output_dir}/PLAIN.csv'

# Create the output directory using dbutils.fs
dbutils.fs.mkdirs(output_dir)

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(f'/dbfs{output_csv_path}', index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd

# Define the source PDF path and output CSV path
source_pdf_path = 'dbfs:/mnt/source_blob/PLANE.pdf'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_path = '/dbfs/mnt/raw_datalake/PLAIN/PLAIN.csv'

# Create the output directory using dbutils.fs
dbutils.fs.mkdirs(output_dir)

# Convert the PDF to CSV
try:
    # Read PDF into a list of DataFrames
    dfs = tabula.read_pdf(source_pdf_path, pages='all', multiple_tables=True)

    # Concatenate all DataFrames into a single DataFrame
    pdf_df = pd.concat(dfs)

    # Remove rows where all elements are NaN
    pdf_df.dropna(how='all', inplace=True)

    # Reset index to make the DataFrame consistent
    pdf_df.reset_index(drop=True, inplace=True)

    # Save the DataFrame as a CSV file
    pdf_df.to_csv(output_csv_path, index=False)

    print(f"PDF successfully converted to CSV at: {output_csv_path}")
except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

pip install pdfplumber

# COMMAND ----------

import pdfplumber
import pandas as pd
import os
from datetime import date

# Define paths
pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
today = str(date.today())
output_dir = f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={today}/'
output_path = os.path.join(output_dir, 'PLAIN.csv')

# Ensure output directory exists
dbutils.fs.mkdirs(output_dir)

# Initialize an empty DataFrame to store the table data
all_tables = []

# Open and read the PDF file
with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            df = pd.DataFrame(table[1:], columns=table[0])  # Convert each table to a DataFrame
            all_tables.append(df)

# Concatenate all tables into a single DataFrame
final_df = pd.concat(all_tables, ignore_index=True)

# Save the DataFrame as a CSV file in the output directory
final_df.to_csv(output_path, index=False)

print(f"CSV file saved at: {output_path}")


# COMMAND ----------

import pdfplumber
import pandas as pd
import os
from datetime import date

# Define paths
pdf_path = '/dbfs/mnt/source_blob/PLANE.pdf'
today = str(date.today())
output_dir = f'/mnt/raw_datalake/PLAIN/Date_Part={today}/'
output_path = f'/dbfs{output_dir}PLAIN.csv'

# Ensure output directory exists in the Databricks File System (DBFS) format
dbutils.fs.mkdirs(output_dir)

# Initialize an empty DataFrame to store the table data
all_tables = []

# Open and read the PDF file
with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            df = pd.DataFrame(table[1:], columns=table[0])  # Convert each table to a DataFrame
            all_tables.append(df)

# Concatenate all tables into a single DataFrame
final_df = pd.concat(all_tables, ignore_index=True)

# Save the DataFrame as a CSV file in the output directory
final_df.to_csv(output_path, index=False)

print(f"CSV file saved at: {output_path}")


# COMMAND ----------

# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2

# Determine the total number of pages in the PDF
pdf_file = open('/dbfs/mnt/source_blob/PLANE.pdf', 'rb')
pdf_reader = PyPDF2.PdfFileReader(pdf_file)
total_pages = pdf_reader.numPages
pdf_file.close()

# Initialize an empty list to store DataFrames
all_dfs = []

# Read each page separately
for page in range(1, total_pages + 1):
    dfs = read_pdf('/dbfs/mnt/source_blob/PLANE.pdf', pages=page)
    if isinstance(dfs, list):
        all_dfs.extend(dfs)
    else:
        all_dfs.append(dfs)

# Extract header from the first DataFrame
header_df = all_dfs[0]
header = header_df.columns.tolist()

# Apply the header to all DataFrames
for i in range(1, len(all_dfs)):
    all_dfs[i].columns = header

# Concatenate all DataFrames
combined_df = pd.concat(all_dfs, ignore_index=True)

# Save the combined DataFrame to CSV
combined_df.to_csv('/dbfs/mnt/raw_datalake/output.csv', index=False)


# COMMAND ----------

# MAGIC %pip install PyPDF2 pandas
# MAGIC
# MAGIC import PyPDF2
# MAGIC import pandas as pd
# MAGIC
# MAGIC # Function to extract text from PDF
# MAGIC def extract_text_from_pdf(pdf_path):
# MAGIC     pdf_reader = PyPDF2.PdfFileReader(pdf_path)
# MAGIC     text = []
# MAGIC     for page_num in range(pdf_reader.numPages):
# MAGIC         page = pdf_reader.getPage(page_num)
# MAGIC         text.append(page.extract_text())
# MAGIC     return text
# MAGIC
# MAGIC # Example paths (adjust these paths based on your mount points)
# MAGIC pdf_path = "/mnt/source/path/to/your/file.pdf"
# MAGIC csv_path = "/mnt/sink/path/to/your/output.csv"
# MAGIC
# MAGIC # Extract text from PDF
# MAGIC pdf_text = extract_text_from_pdf(pdf_path)
# MAGIC
# MAGIC # Process the text to extract header and data
# MAGIC header = pdf_text[0].split('\n')[0]  # Assuming the header is the first line of the first page
# MAGIC data = []
# MAGIC
# MAGIC for page in pdf_text:
# MAGIC     lines = page.split('\n')
# MAGIC     for line in lines[1:]:  # Skip the header line on the first page
# MAGIC         data.append(line.split())  # Adjust the split logic based on your table structure
# MAGIC
# MAGIC # Create DataFrame
# MAGIC columns = header.split()  # Adjust the split logic based on your header structure
# MAGIC df = pd.DataFrame(data, columns=columns)
# MAGIC
# MAGIC # Save DataFrame to CSV
# MAGIC df.to_csv(csv_path, index=False)
# MAGIC
# MAGIC print(f"CSV file saved to {csv_path}")
