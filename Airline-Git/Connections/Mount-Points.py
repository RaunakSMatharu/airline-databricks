# Databricks notebook source
# MAGIC %scala
# MAGIC val containerName=dbutils.secrets.get(scope="raunak-secret-scope",key="source-blob-container-name") //source
# MAGIC val storageAccountName=dbutils.secrets.get(scope="raunak-secret-scope",key="source-blob-storage-name") //storageaccsource 
# MAGIC val sas =dbutils.secrets.get(scope="raunak-secret-scope",key="source-token-sas") //SAS TOKEN
# MAGIC val config ="fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
# MAGIC dbutils.fs.mount(
# MAGIC   source="wasbs://plane@sourceaccblob.blob.core.windows.net",//wasbs://containername@accountname.blob.core.windows.net  
# MAGIC   mountPoint="/mnt/sourceblob/",
# MAGIC   extraConfigs= Map(config -> sas)
# MAGIC )
# MAGIC

# COMMAND ----------

# Function to fetch secret
def f_get_secret(key):
    return dbutils.secrets.get(scope="raunak-secret-scope", key=key)

# Configuration dictionary with OAuth credentials
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f_get_secret(key="app-id"),
    "fs.azure.account.oauth2.client.secret": f_get_secret(key="app-secret"),
    "fs.azure.account.oauth2.client.endpoint": f_get_secret(key="client-endpoint")
}


# COMMAND ----------

mountPoint = "/mnt/sinkstoragegen2acc/"

# Mount the ADLS Gen2 storage account
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source="abfss://raw@sinkgen2acc.dfs.core.windows.net/",
        mount_point=mountPoint,
        extra_configs=configs
    )

# COMMAND ----------

mountPoint = "/mnt/cleanstoragegen2acc/"

# Mount the ADLS Gen2 storage account
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source="abfss://cleansed@sinkgen2acc.dfs.core.windows.net/",
        mount_point=mountPoint,
        extra_configs=configs
    )

# COMMAND ----------

dbutils.fs.ls('/mnt/cleanstoragegen2acc/')

# COMMAND ----------

dbutils.fs.ls('/mnt/sinkstoragegen2acc/')

# COMMAND ----------


