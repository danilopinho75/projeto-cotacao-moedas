# Databricks notebook source
storage_account = 'adlsprojetocotacao'

# COMMAND ----------

df_moedas_silver = spark.read.parquet(f"/mnt/{storage_account}/bronze/moedas/")
display(df_moedas_silver)

# COMMAND ----------

# Transformar nomes de colunas
df_moedas_silver = df_moedas_silver.selectExpr(
    "nomeFormatado AS MoedaNome",
    "simbolo AS Moeda"
)
display(df_moedas_silver)

# COMMAND ----------

# Carregar na camada silver
df_moedas_silver.write.mode("overwrite").format("delta").save(f"/mnt/{storage_account}/silver/moedas/")
