# Databricks notebook source
storage_account = 'adlsprojetocotacao'

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/mnt/{storage_account}/bronze/cotacoes/"))

# COMMAND ----------

# Leitura dos arquivos parquet na camada bronze
df_cotacoes_silver = spark.read.parquet(f"dbfs:/mnt/{storage_account}/bronze/cotacoes/*.parquet")

df_cotacoes_silver.createOrReplaceTempView("vw_cotacoes_silver")

# Transformação do dataframe de cotações
df_cotacoes_silver = spark.sql("""
    SELECT
        cotacaoCompra AS Cotacao,
        CAST(dataHoraCotacao AS DATE) AS Data,
        moeda AS Moeda
    FROM
        vw_cotacoes_silver
    ORDER BY Data ASC                            
""")

display(df_cotacoes_silver)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.cotacoes (
# MAGIC   Cotacao DOUBLE,
# MAGIC   Data DATE,
# MAGIC   Moeda STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Moeda)

# COMMAND ----------

# Executa o merge na tabela cotacoes
df_cotacoes_silver.createOrReplaceTempView("df_novos")

spark.sql("""
          MERGE INTO silver.cotacoes AS e
          USING (
              SELECT 
                Cotacao,
                Data,
                Moeda
            FROM df_novos
          ) AS n
          ON e.Moeda = n.Moeda
            AND e.Data = n.Data
        WHEN NOT MATCHED THEN
            INSERT (Cotacao, Data, Moeda)
            VALUES (n.Cotacao, n.Data, n.Moeda)
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS quantidade_linhas
# MAGIC FROM silver.cotacoes

# COMMAND ----------

df_cotacao_silver = spark.sql("SELECT * FROM silver.cotacoes")
display(df_cotacao_silver)

# COMMAND ----------

# Carregar na camada silver
df_cotacao_silver.write.format("delta").mode("overwrite").save(f"dbfs:/mnt/{storage_account}/silver/cotacoes/")

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/mnt/{storage_account}/silver/cotacoes/"))

# COMMAND ----------


