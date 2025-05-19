# Databricks notebook source
#ADLS
storage_account_name = 'adlsprojetocotacao'

# App Regristraion
client_id = dbutils.secrets.get('azure-keyvault', 'client-id')
tenant_id = dbutils.secrets.get('azure-keyvault', 'tenant-id')
client_secret = dbutils.secrets.get('azure-keyvault', 'client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point= f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs
    )

# COMMAND ----------

# Mount Bronze Layer
mount_adls('bronze')

# COMMAND ----------

# Mount Silver Layer
mount_adls('silver')

# COMMAND ----------

# Mount Gold Layer
mount_adls('gold')

# COMMAND ----------

display(dbutils.fs.mounts())
