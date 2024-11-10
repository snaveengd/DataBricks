# Databricks notebook source
for i in dbutils.fs.ls('/mnt/source_blob/') :
  print(i)

# COMMAND ----------

files_pdf=[i.name for i in dbutils.fs.ls('/mnt/source_blob/')]

print(files_pdf)

# COMMAND ----------

files_pdf=[(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/')]

print(files_pdf)

# COMMAND ----------

files_pdf=[(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]

print(files_pdf)

# COMMAND ----------

# MAGIC %pip install tabula-py

# COMMAND ----------

# MAGIC %pip install jpype1

# COMMAND ----------

from datetime import date

dbutils.fs.mkdirs("/dbfs/mnt/raw_datalake/PLAIN/Date_Part={date.today()}/")

# COMMAND ----------

import tabula
from datetime import date
def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
  try:
    dbutils.fs.mkdirs(f"{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
    tabula.convert_into(f'{source_path}{file_name}',f"/dbfs{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
  except Exception as e:
    print(f'Error processing {file_name}: {e}')


# COMMAND ----------

list_files=[(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]

for i in list_files:
  f_source_pdf_datalake('/dbfs/mnt/source_blob/','/mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------

import tabula
from datetime import date
def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
  try:
    dbutils.fs.mkdirs(f"{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
    tabula.convert_into(f'{source_path}{file_name}',f"/dbfs{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
  except Exception as e:
    print(f'Error processing {file_name}: {e}')
    
list_files=[(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if(i.name.split('.')[1]=='pdf')]

for i in list_files:
  f_source_pdf_datalake('/dbfs/mnt/source_blob/','/mnt/raw_datalake/','csv','all',i[0])    
