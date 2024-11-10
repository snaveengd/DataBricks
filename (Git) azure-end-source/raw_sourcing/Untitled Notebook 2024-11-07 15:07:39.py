# Databricks notebook source
# Install required libraries
%pip install tabula-py
%pip install jpype1
%pip install PyPDF2

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

# Install required libraries
%pip install tabula-py
%pip install jpype1
%pip install PyPDF2

# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2

# Determine the total number of pages in the PDF
pdf_file = open('/dbfs/mnt/source_blob/PLANE.pdf', 'rb')
pdf_reader = PyPDF2.PdfReader(pdf_file)
total_pages = len(pdf_reader.pages)
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



# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2

# Determine the total number of pages in the PDF
pdf_file = open('/dbfs/mnt/source_blob/PLANE.pdf', 'rb')
pdf_reader = PyPDF2.PdfReader(pdf_file)
total_pages = len(pdf_reader.pages)
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

# Create the directory if it doesn't exist
dbutils.fs.mkdirs('/dbfs/mnt/raw_datalake/')

# Save the combined DataFrame to CSV
combined_df.to_csv('/dbfs/mnt/raw_datalake/output.csv', index=False)

# COMMAND ----------

# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2

# Determine the total number of pages in the PDF
pdf_file = open('/dbfs/mnt/source_blob/PLANE.pdf', 'rb')
pdf_reader = PyPDF2.PdfReader(pdf_file)
total_pages = len(pdf_reader.pages)
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

# Create the directory if it doesn't exist
dbutils.fs.mkdirs('dbfs:/mnt/raw_datalake/plain')

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
# MAGIC pdf_path = "/mnt/source_blob/PLANE.pdf"
# MAGIC csv_path = "/mnt/raw_datalake/PLAIN/output.csv"
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

# COMMAND ----------



import PyPDF2
import pandas as pd

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    pdf_reader = PyPDF2.PdfReader(pdf_path)  # Updated to use PdfReader
    text = []
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        text.append(page.extract_text())
    return text

# Example paths (adjust these paths based on your mount points)
pdf_path = "/mnt/source_blob/PLANE.pdf"
csv_path = "/mnt/raw_datalake/PLAIN/output.csv"

# Extract text from PDF
pdf_text = extract_text_from_pdf(pdf_path)

# Process the text to extract header and data
header = pdf_text[0].split('\n')[0]  # Assuming the header is the first line of the first page
data = []

for page in pdf_text:
    lines = page.split('\n')
    for line in lines[1:]:  # Skip the header line on the first page
        data.append(line.split())  # Adjust the split logic based on your table structure

# Create DataFrame
columns = header.split()  # Adjust the split logic based on your header structure
df = pd.DataFrame(data, columns=columns)

# Save DataFrame to CSV
df.to_csv(csv_path, index=False)

print(f"CSV file saved to {csv_path}")

# COMMAND ----------

# MAGIC %pip install koalas

# COMMAND ----------

import PyPDF2
import pandas as pd
from io import BytesIO
import databricks.koalas as ks

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    pdf_file = ks.read_binary_file(pdf_path)  # Use Koalas to read the binary file
    pdf_bytes = BytesIO(pdf_file)  # Convert to BytesIO object
    pdf_reader = PyPDF2.PdfReader(pdf_bytes)  # Updated to use PdfReader with BytesIO object
    text = []
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        text.append(page.extract_text())
    return text

# Example paths (adjust these paths based on your mount points)
pdf_path = "dbfs:/mnt/source_blob/PLANE.pdf"  # Ensure the path is prefixed with 'dbfs:'
csv_path = "/dbfs/mnt/raw_datalake/PLAIN/output.csv"  # Prefix with '/dbfs' for local file API access

# Extract text from PDF
pdf_text = extract_text_from_pdf(pdf_path)

# Process the text to extract header and data
header = pdf_text[0].split('\n')[0]  # Assuming the header is the first line of the first page
data = []

for page in pdf_text:
    lines = page.split('\n')
    for line in lines[1:]:  # Skip the header line on the first page
        data.append(line.split())  # Adjust the split logic based on your table structure

# Create DataFrame
columns = header.split()  # Adjust the split logic based on your header structure
df = pd.DataFrame(data, columns=columns)

# Save DataFrame to CSV
df.to_csv(csv_path, index=False)

print(f"CSV file saved to {csv_path}")

# COMMAND ----------

# MAGIC %pip install pyspark
# MAGIC
# MAGIC import pandas as pd
# MAGIC from io import BytesIO
# MAGIC from pyspark.pandas import read_binary_file  # Import the correct function
# MAGIC import PyPDF2
# MAGIC
# MAGIC # Function to extract text from PDF
# MAGIC def extract_text_from_pdf(pdf_path):
# MAGIC     pdf_file = read_binary_file(pdf_path)  # Use Pandas API on Spark to read the binary file
# MAGIC     pdf_bytes = BytesIO(pdf_file.to_pandas().iloc[0, 0])  # Convert to BytesIO object
# MAGIC     pdf_reader = PyPDF2.PdfReader(pdf_bytes)  # Updated to use PdfReader with BytesIO object
# MAGIC     text = []
# MAGIC     for page_num in range(len(pdf_reader.pages)):
# MAGIC         page = pdf_reader.pages[page_num]
# MAGIC         text.append(page.extract_text())
# MAGIC     return text
# MAGIC
# MAGIC # Example paths (adjust these paths based on your mount points)
# MAGIC pdf_path = "dbfs:/mnt/source_blob/PLANE.pdf"  # Ensure the path is prefixed with 'dbfs:'
# MAGIC csv_path = "/dbfs/mnt/raw_datalake/PLAIN/output.csv"  # Prefix with '/dbfs' for local file API access
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

# COMMAND ----------

# MAGIC %pip install pyspark PyPDF2
# MAGIC
# MAGIC import pandas as pd
# MAGIC from io import BytesIO
# MAGIC from pyspark.sql import SparkSession  # Import SparkSession
# MAGIC import PyPDF2
# MAGIC
# MAGIC # Create SparkSession
# MAGIC spark = SparkSession.builder.appName("PDF Processing").getOrCreate()
# MAGIC
# MAGIC # Function to extract text from PDF
# MAGIC def extract_text_from_pdf(pdf_path):
# MAGIC     # Read the binary file using Spark
# MAGIC     pdf_df = spark.read.format("binaryFile") \
# MAGIC         .load(pdf_path) \
# MAGIC         .select("content")
# MAGIC     
# MAGIC     # Convert the binary content to a BytesIO object
# MAGIC     pdf_bytes = BytesIO(pdf_df.collect()[0].content)
# MAGIC     
# MAGIC     # Use PyPDF2 to read the PDF from BytesIO object
# MAGIC     pdf_reader = PyPDF2.PdfReader(pdf_bytes)
# MAGIC     text = []
# MAGIC     for page_num in range(len(pdf_reader.pages)):
# MAGIC         page = pdf_reader.pages[page_num]
# MAGIC         text.append(page.extract_text())
# MAGIC     return text
# MAGIC
# MAGIC # Example paths (adjust these paths based on your mount points)
# MAGIC pdf_path = "dbfs:/mnt/source_blob/PLANE.pdf"  # Ensure the path is prefixed with 'dbfs:'
# MAGIC csv_path = "/dbfs/mnt/raw_datalake/PLAIN/output.csv"  # Prefix with '/dbfs' for local file API access
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

# COMMAND ----------

# Install required libraries
%pip install tabula-py
%pip install jpype1
%pip install PyPDF2

# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2
from datetime import date

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
combined_df.to_csv(f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={date.today()}/PLAIN.csv', index=False)


# COMMAND ----------

# Install required libraries
%pip install tabula-py
%pip install jpype1
%pip install PyPDF2

# Import libraries
import pandas as pd
from tabula import read_pdf
import PyPDF2
from datetime import date

# Determine the total number of pages in the PDF
pdf_file = open('/dbfs/mnt/source_blob/PLANE.pdf', 'rb')
pdf_reader = PyPDF2.PdfReader(pdf_file)
total_pages = len(pdf_reader.pages)
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
combined_df.to_csv(f'/dbfs/mnt/raw_datalake/PLAIN/Date_Part={date.today()}/PLAIN.csv', index=False)


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

import tabula
from datetime import date
print(date.today())
today = str(date.today())
output_dir = f'/dbfs/mnt/raw_datalake/PLAlN/Datepart={today}'
dbutils.fs.mkdirs(output_dir)

# Read PDF into a DataFrame
df = pd.concat(tabula.read_pdf('/dbfs/mnt/source_blob/PLANE.pdf', pages='all'), ignore_index=True)

# Convert DataFrame to CSV and save to the output directory
output_path = f'{output_dir}/PLAIN.csv'
df.to_csv(f'/dbfs{output_path}', index=False)

# Display the DataFrame
#display(df)

# COMMAND ----------



import tabula
import pandas as pd
from datetime import date

print(date.today())
today = str(date.today())
output_dir = f'/dbfs/mnt/raw_datalake/PLAlN/Datepart={today}'
dbutils.fs.mkdirs(output_dir)

# Read PDF into a list of DataFrames
df_list = tabula.read_pdf('/dbfs/mnt/source_blob/PLANE.pdf', pages='all')

# Concatenate the list of DataFrames into a single DataFrame
df = pd.concat(df_list, ignore_index=True)

# Convert DataFrame to CSV and save to the output directory
output_path = f'{output_dir}/PLAIN.csv'
df.to_csv(f'/dbfs{output_path}', index=False)

# Display the DataFrame
display(df)

# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd
import os

# Define the source and output paths
source_dir = '/dbfs/mnt/source_blob/'
output_dir = '/dbfs/mnt/raw_datalake/PLAIN'
output_csv_filename = 'PLAIN.csv'

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# List the files in the source directory
source_files = [file.name for file in dbutils.fs.ls(source_dir)]

# Filter to get only PDF files
pdf_files = [file for file in source_files if file.endswith('.pdf')]

# Process each PDF file
for pdf_file in pdf_files:
    # Define the full source path and the output CSV path
    source_pdf_path = os.path.join(source_dir, pdf_file)
    output_csv_path = os.path.join(output_dir, output_csv_filename)
    
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
        
        print(f"PDF {pdf_file} successfully converted to CSV at: {output_csv_path}")
    except Exception as e:
        print(f"An error occurred while processing {pdf_file}: {e}")


# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd

# Define the source and output paths
source_dir = '/dbfs/mnt/source_blob/'
output_dir = '/dbfs/mnt/raw_datalake/PLAIN'
output_csv_filename = 'PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# List the files in the source directory
source_files = [file.name for file in dbutils.fs.ls(source_dir)]

# Filter to get only PDF files
pdf_files = [file for file in source_files if file.endswith('.pdf')]

# Process each PDF file
for pdf_file in pdf_files:
    # Define the full source path and the output CSV path
    source_pdf_path = f"{source_dir}{pdf_file}"
    output_csv_path = f"{output_dir}/{output_csv_filename}"
    
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
        
        print(f"PDF {pdf_file} successfully converted to CSV at: {output_csv_path}")
    except Exception as e:
        print(f"An error occurred while processing {pdf_file}: {e}")

# COMMAND ----------

# Define the source and output paths without the /dbfs prefix
source_dir = '/mnt/source_blob/'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_filename = 'PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# List the files in the source directory
source_files = [file.name for file in dbutils.fs.ls(source_dir)]

# Continue with the rest of your code...

# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd

# Define the source and output paths without the /dbfs prefix
source_dir = '/mnt/source_blob/'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_filename = 'PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# List the files in the source directory
source_files = [file.name for file in dbutils.fs.ls(source_dir)]

# Filter to get only PDF files
pdf_files = [file for file in source_files if file.endswith('.pdf')]

# Process each PDF file
for pdf_file in pdf_files:
    # Define the full source path and the output CSV path
    source_pdf_path = f"{source_dir}{pdf_file}"
    output_csv_path = f"{output_dir}/{output_csv_filename}"
    
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
        
        print(f"PDF {pdf_file} successfully converted to CSV at: {output_csv_path}")
    except Exception as e:
        print(f"An error occurred while processing {pdf_file}: {e}")

# Display the DataFrame
display(pdf_df)

# COMMAND ----------

# Install tabula-py if not already installed
%pip install tabula-py

import tabula
import pandas as pd

# Define the source and output paths without the /dbfs prefix
source_dir = '/mnt/source_blob/'
output_dir = '/mnt/raw_datalake/PLAIN'
output_csv_filename = 'PLAIN.csv'

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# List the files in the source directory
source_files = [file.name for file in dbutils.fs.ls(source_dir)]

# Filter to get only PDF files
pdf_files = [file for file in source_files if file.endswith('.pdf')]

# Initialize pdf_df to None
pdf_df = None

# Process each PDF file
for pdf_file in pdf_files:
    # Define the full source path and the output CSV path
    source_pdf_path = f"{source_dir}{pdf_file}"
    output_csv_path = f"{output_dir}/{output_csv_filename}"
    
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
        
        print(f"PDF {pdf_file} successfully converted to CSV at: {output_csv_path}")
    except Exception as e:
        print(f"An error occurred while processing {pdf_file}: {e}")

# Display the DataFrame if it was created
if pdf_df is not None:
    display(pdf_df)
else:
    print("No PDF files were processed.")
