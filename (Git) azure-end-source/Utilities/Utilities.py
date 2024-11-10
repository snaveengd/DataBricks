# Databricks notebook source
def pre_schema(df):
  try:
    schema=''
    for i in df.dtypes:
        schema= schema+i[0]+' '+i[1]+','
    return schema[0:-1] # to remove , at the end
  except Exception as err:
    print("Error occured",str(err))

# COMMAND ----------

def pre_schema(df):
  try:
    schema=''
    for i in df.dtypes:
        schema= schema+i[0]+' '+i[1]+','
    return schema[0:-1] # to remove , at the end
  except Exception as err:
    print("Error occured",str(err))
	
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

def f_count_check(database,operation_type,table_name,number_diff):
 
    spark.sql(f"""DESC HISTORY {database}.{table_name}""").createOrReplaceTempView("Table_count")


    count_current=spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version=(select max(version) from Table_count where trim(lower(operation))=lower('{operation_type}'))""")
    if(count_current.first() is None ):
      final_count_current=0
    else:
        final_count_current=int(count_current.first().numOutputRows)
    count_previous=spark.sql("""select operationMetrics.numOutputRows from Table_count where version<(select version from Table_count where trim(lower(operation))=lower('{operation_type}') order by version desc limit 1)""")
    if(count_previous.first() is None ):
      final_count_previous=0
    else:
        final_count_previous=int(count_previous.first().numOutputRows)
    if((final_count_current - final_count_previous) > number_diff):
      print("Differnce is huge", table_name)
      raise Exception("Differnce is huge", table_name) 
    else:
      pass
          
