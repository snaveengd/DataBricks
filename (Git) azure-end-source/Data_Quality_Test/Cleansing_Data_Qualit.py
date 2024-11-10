# Databricks notebook source
# MAGIC %run /Workspace/Users/navee.shiga@gmail.com/azure-end-source/Utilities/Utilities

# COMMAND ----------

list_table_info=[('write','airline',100),('write','Plane',100)]

for i in list_table_info:
  f_count_check('cleansed_EndtoEnd',i[0],i[1],i[2])
