# Databricks notebook source
import requests
import json
import datetime
from pyspark.sql.functions import lit

# COMMAND ----------

storage_account = 'adlsprojetocotacao'

# COMMAND ----------

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# COMMAND ----------

df_moedas = spark.sql("SELECT DISTINCT Moeda FROM dim_moedas")
linhas = df_moedas.collect()
lista_moedas = []

for linha in linhas:
    lista_moedas.append(linha.Moeda)

print(lista_moedas)

# COMMAND ----------

# Variáveis

data_inicial = '01-02-2020'
data_final = datetime.date.today().strftime('%m-%d-%Y')
top = 100

for moeda in lista_moedas:

    skip = 0

    todos_dados = []

    while True:
        url = (f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
            f"@moeda='{moeda}'&@dataInicial='{data_inicial}'&@dataFinalCotacao='{data_final}'"
            f"&$top={top}&$skip={skip}&$filter=tipoBoletim%20eq%20'Fechamento'&$format=json&$select=cotacaoCompra,dataHoraCotacao"
            )
        
        response = requests.get(url, verify=False)

        # Verifica se a resposta foi bem-sucedida (HTTP 200)
        if response.status_code == 200:
            try:
                dados_json = response.json()
                dados = dados_json.get('value', [])
            except ValueError:
                print(f"❌ Erro ao decodificar JSON para a moeda {moeda} com skip={skip}")
                print("Conteúdo da resposta:", response.text)
                break
        else:
            print(f"❌ Erro HTTP {response.status_code} para a moeda {moeda} com skip={skip}")
            print("URL usada:", url)
            break


        if not dados:
            break

        todos_dados.extend(dados)

        skip+=top

    if todos_dados:
        df = spark.createDataFrame(todos_dados) \
            .withColumn('moeda', lit(moeda))

    data_inicial_path = datetime.datetime.strptime(data_inicial, '%m-%d-%Y').strftime('%Y-%m-%d')
    data_final_path = datetime.datetime.strptime(data_final, '%m-%d-%Y').strftime('%Y-%m-%d')

    path = (
        f"/mnt/{storage_account}/bronze/cotacoes/"
        f"{moeda}_"
        f"{data_inicial_path}_"
        f"{data_final_path}"
        f".parquet"
    )

    df.write.mode('overwrite').parquet(path)

# COMMAND ----------

files = dbutils.fs.ls(f"/mnt/{storage_account}/bronze/cotacoes")
display(files)
