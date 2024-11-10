# Databricks notebook source
# Hardcoded values for storage configuration
container_name = "source"
storage_account_name = "azureenddev01sa"
sas_token = "sp=racwdl&st=2024-11-05T03:17:09Z&se=2024-11-19T11:17:09Z&spr=https&sv=2022-11-02&sr=c&sig=GIgw4fji%2FIUMO5Ej8KpXDneVaHj2KgwC%2FNkIBLubRw0%3D"

# Construct the Blob Storage URL and config key
blob_mount_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net"
config_key = f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net"

# Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=blob_mount_path,
        mount_point="/mnt/source_blob/",
        extra_configs={config_key: sas_token}
    )
    print("Mount successful!")
except Exception as e:
    print(f"Error mounting Blob Storage: {e}")


# COMMAND ----------

dbutils.fs.ls("/mnt/source_blob/")

# COMMAND ----------

# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://raw@azuredev01sink.dfs.core.windows.net/",
  mount_point = '/mnt/raw_datalake/',
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

# mounting cleansed container
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://cleansed@azuredev01sink.dfs.core.windows.net/",
  mount_point = '/mnt/cleansed_datalake/',
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/cleansed_datalake/")
