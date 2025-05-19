# Databricks notebook source
storage_account = 'adlsprojetocotacao'

# COMMAND ----------

df_moedas_gold = spark.read.format("delta").load(f"/mnt/{storage_account}/silver/moedas/")
display(df_moedas_gold)

# COMMAND ----------

# Salvar na camada gold e como uma tabela dimensão
df_moedas_gold.write.format("delta").mode("overwrite").save(f"/mnt/{storage_account}/gold/moedas/")
df_moedas_gold.write.format("delta").mode("overwrite").saveAsTable("dim_moedas")
