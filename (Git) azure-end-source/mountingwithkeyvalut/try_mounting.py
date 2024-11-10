# Databricks notebook source
# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://source@azureenddev01sa.dfs.core.windows.net/",
  mount_point = "/mnt/source",
  extra_configs = configs)

# COMMAND ----------

# Retrieve secrets from the Databricks secret scope
container_name = dbutils.secrets.get(scope="azure-deve-sink-databricks2", key="container-name")
storage_account_name = dbutils.secrets.get(scope="azure-deve-sink-databricks2", key="storageAccountName")
sas_token = dbutils.secrets.get(scope="azure-deve-sink-databricks2", key="sas3")
blob_mount_path = dbutils.secrets.get(scope="azure-deve-sink-databricks2", key="blob-mnt-path2")

# Construct the configuration key for SAS-based authentication
config_key = f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net"

# Mount the Blob Storage container
dbutils.fs.mount(
    source=blob_mount_path,
    mount_point="/mnt/source_blob/",
    extra_configs={config_key: sas_token}
)


# COMMAND ----------

# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

mount_point = "/mnt/source"

# Unmount if the mount point already exists
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mount_point)

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://source@azureenddev01sa.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs)
