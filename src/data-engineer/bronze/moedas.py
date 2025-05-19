# Databricks notebook source
import requests

# COMMAND ----------

storage_account = 'adlsprojetocotacao'

# COMMAND ----------

url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=100&$format=json&$select=simbolo,nomeFormatado"

response = requests.get(url)
dados = response.json()['value']

df_moedas = spark.createDataFrame(dados)

display(df_moedas)

df_moedas.write.mode("overwrite").parquet(f"/mnt/{storage_account}/bronze/moedas")
