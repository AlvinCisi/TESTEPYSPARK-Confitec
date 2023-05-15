from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, when, col, to_timestamp, to_date, lpad
from datetime import datetime
import boto3
import os
import csv
from dateutil import parser

# Iniciando sessão do Spark
spark = SparkSession.builder.appName('TesteConfitec').getOrCreate()

# Lendo o arquivo .parquet
df = spark.read.parquet('c:/Users/alvin/Desktop/Projetos/TesteConfitec/OriginaisNetflix - Python.parquet')

# Transformações nos dados
df_selected = df.select('Title', 'Genre', 'Seasons', 'Premiere', 'Language', 'Active', 'Status', 'dt_inclusao')

df_selected = df_selected.withColumn('Premiere', lpad(col('Premiere'), 9, '0'))
df_selected = df_selected.withColumn('premiere', to_date(col('premiere'), 'dd-MMM-yy'))
df_selected = df_selected.withColumn('dt_inclusao', to_timestamp(col('dt_inclusao'), 'yyyy-MM-dd\'T\'HH:mm:ss'))

df_selected = df_selected.dropDuplicates()

df_selected = df_selected.withColumn('Seasons', when(col('Seasons') == 'TBA', 'a ser anunciado').otherwise(col('Seasons')))
df_selected = df_selected.withColumn('Data de Alteração', current_timestamp())

df_selected = df_selected.withColumnRenamed('Title', 'Título')
df_selected = df_selected.withColumnRenamed('Genre', 'Gênero')
df_selected = df_selected.withColumnRenamed('Seasons', 'Temporadas')
df_selected = df_selected.withColumnRenamed('Premiere', 'Lançamento')
df_selected = df_selected.withColumnRenamed('Language', 'Idioma')
df_selected = df_selected.withColumnRenamed('Active', 'Ativo')
df_selected = df_selected.withColumnRenamed('Status', 'Status')
df_selected = df_selected.withColumnRenamed('dt_inclusao', 'Data de Inclusão')

df_selected = df_selected.orderBy(col('Ativo').desc(), col('Gênero').desc())

# Escrevendo os dados em um arquivo CSV
with open('OriginaisNetflix-Python.csv', mode='w', newline='') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['Título', 'Gênero', 'Temporadas', 'Lançamento', 'Idioma', 'Ativo', 'Status', 'Data de Inclusão', 'Data de Alteração'])
    for row in df_selected.collect():
        writer.writerow(row)


# Copiando o arquivo gerado para o S3
s3 = boto3.resource('s3',
                    aws_access_key_id='AKIAWGCXGLVUYQIWLGG7',
                    aws_secret_access_key='2OHBB6YJ8LT1lZHcld5xZ4E4Nhx',
                    region_name='sa-east-1')

bucket_name = 'testepysparkconfitec'
file_name = 'OriginaisNetflix-Python.csv'
s3.meta.client.upload_file(file_name, bucket_name, file_name)