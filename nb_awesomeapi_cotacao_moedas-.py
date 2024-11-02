# Databricks notebook source
# MAGIC %md
# MAGIC Conectando com API Contação De moedas

# COMMAND ----------

import requests
import json

cotacoes = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL")
cotacoes_dic = cotacoes.json()
print(cotacoes_dic)



# COMMAND ----------

# MAGIC %md
# MAGIC Código para Criar um DataFrame a partir da Resposta da API

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder.appName("Cotacoes").getOrCreate()

# Obter dados da API
cotacoes = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL")
cotacoes_dic = cotacoes.json()

# Extrair os dados das cotações em uma lista
dados_cotacoes = []
for key, value in cotacoes_dic.items():
    # Adiciona o valor e o código como uma nova linha no DataFrame
    dados_cotacoes.append(value)

# Criar DataFrame a partir dos dados
df_cotacoes = spark.createDataFrame(dados_cotacoes)

# Mostrar o DataFrame
df_cotacoes.show(truncate=False)

# Parar a sessão Spark quando terminar
# spark.stop()  # Descomente esta linha para parar a sessão ao final


# COMMAND ----------

# MAGIC %md
# MAGIC Tratamento schema de dados e salvando o DataFrame como uma tabela no Databricks

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder.appName("Cotacoes").getOrCreate()

# Obter dados da API
cotacoes = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL")
cotacoes_dic = cotacoes.json()

# Extrair os dados das cotações em uma lista
dados_cotacoes = []
for key, value in cotacoes_dic.items():
    # Adiciona o valor e o código como uma nova linha no DataFrame
    dados_cotacoes.append({
        'moeda': value['code'],
        'nome': value['name'],
        'bid': float(value['bid']),
        'ask': float(value['ask']),
        'high': float(value['high']),
        'low': float(value['low']),
        'varBid': float(value['varBid']),
        'pctChange': float(value['pctChange']),
        'timestamp': value['timestamp'],
        'create_date': value['create_date']
    })

# Criar DataFrame a partir dos dados
df_cotacoes = spark.createDataFrame(dados_cotacoes)

# Mostrar o DataFrame
df_cotacoes.show(truncate=False)

# Definir o caminho de armazenamento
output_table_name = "cotacoes_tabela"

# Salvar o DataFrame como uma tabela no Hive metastore
df_cotacoes.write.format("delta").mode("overwrite").saveAsTable(output_table_name)

# Exibir a tabela criada
spark.sql(f"SELECT * FROM {output_table_name}").show(truncate=False)



# COMMAND ----------

import requests
import json
import boto3

# Configurações de acesso ao S3
ACCESS_KEY = 
SECRET_KEY =
BUCKET_NAME = 'awesomeapicotacao'
FOLDER_NAME = 'raw/'
FILE_NAME = 'cotacoes.json'

# Obter dados da API
cotacoes = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL")
cotacoes_dic = cotacoes.json()

# Salvar os dados das cotações em formato JSON no S3
s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# Converter o dicionário para JSON e salvar
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=f'{FOLDER_NAME}{FILE_NAME}',
    Body=json.dumps(cotacoes_dic)
)

print(f'Dados salvos em JSON no bucket {BUCKET_NAME} no caminho {FOLDER_NAME}{FILE_NAME}')


# COMMAND ----------

# MAGIC %md
# MAGIC Granvando no Bucket S3: "s3a://awesomeapicotacao/gold/" e salvado em delta parquet

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

# Inicializar SparkSession
spark = SparkSession.builder.appName("Cotacoes").getOrCreate()

# Configurar as credenciais do S3 diretamente no código (não recomendado para produção)
spark.conf.set("fs.s3a.access.key", "")
spark.conf.set("fs.s3a.secret.key", "")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# Obter dados da API de cotações
cotacoes = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL")
cotacoes_dic = cotacoes.json()

# Extrair os dados das cotações em uma lista
dados_cotacoes = []
for key, value in cotacoes_dic.items():
    dados_cotacoes.append({
        'moeda': value['code'],
        'nome': value['name'],
        'bid': float(value['bid']),
        'ask': float(value['ask']),
        'high': float(value['high']),
        'low': float(value['low']),
        'varBid': float(value['varBid']),
        'pctChange': float(value['pctChange']),
        'timestamp': value['timestamp'],
        'create_date': value['create_date']
    })

# Criar DataFrame a partir dos dados
df_cotacoes = spark.createDataFrame(dados_cotacoes)

# Mostrar o DataFrame
df_cotacoes.show(truncate=False)

# Salvar no S3 no formato Delta Parquet
caminho_s3 = "s3a://awesomeapicotacao/gold/"
df_cotacoes.write.format("delta").mode("overwrite").save(caminho_s3)

print(f"Dados salvos no S3 no caminho: {caminho_s3}")

