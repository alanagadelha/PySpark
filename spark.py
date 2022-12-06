################################################
# Written by Andre Dantas and Alana Gadelha    #
# Date 2022, 26th of NOV                       #
# Disciplina: Bancos de Dados distribuidos     #
# Professor: Daniel Cardoso Moraes de Oliveira #
################################################
from datetime import datetime
import os, sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

# Create SparkSession 
spark = SparkSession.builder.appName("BancoDeDadosDistribuido-2022").getOrCreate() 
print(spark)
# Create DataFrame from PySpark
# DataTime for inition processing
start_global_time = datetime.now()
df = spark.read.csv('Compras-formatado.csv', header=True, inferSchema=True)
print(type(df))
df.printSchema()
print('')
print('')
print(f'Time start time in GLOBAL PROGRAM (hh:mm:ss.ms) is:', start_global_time)
print('')
print('')
#### Starting Query 1###########################################################
print('=======================================================================')
print('Starting Query 1 - Qual o total de itens comprados por cada cliente?')
start = datetime.now()
df2 = df.groupBy('nome', 'produto', 'quantidade').count().show(50)
df2 = df.select('nome', 'produto', 'quantidade').describe().show(500)
end = datetime.now()
print('The start execution time of Query 1 is:',start)
print('The end execution time of Query 1 is:',end)
print(f"Time taken in Query 1 (hh:mm:ss.ms) is {end - start}")
print('=======================================================================')
####Finishing Query 1###########################################################

####Starting Query 2###########################################################
print('=======================================================================')
print('Starting Query 2 - Qual o valor total vendido por loja por mês/ano?')
start = datetime.now()
df.select(col("endereco_ip"), col("data"), year(col("data")).alias("year"), month(col("data")).alias("month")).show(50)
end = datetime.now()
print('The start execution time of Query 2 is:',start)
print('The end execution time of Query 2 is:',end)
print(f"Time taken in Query 2 (hh:mm:ss.ms) is {end - start}")
print('=======================================================================')
####Finishing Query 2###########################################################

####Starting Query 3###########################################################
print('=======================================================================')
print('Starting Query 3 - Qual o valor total vendido por cada vendedor em 2022')
print('ordenado por valor total vendido decrescente?')
start = datetime.now()
#df3 = df.withColumn('data', f.to_date(df.data.cast(StringType()),'ddMMyyyy'))
df3 = df[(df['data']>pd.Timestamp(2022,1,1)) & (df['data']<pd.Timestamp(2022,12,31))]
df3.sort(df.valor.desc()).groupBy('vendedor_nome', 'data', 'valor').count().show(100)
end = datetime.now()
print('The start execution time of Query 3 is:',start)
print('The end execution time of Query 3 is:',end)
print(f"Time taken in Query 3 (hh:mm:ss.ms) is {end - start}")
print('=======================================================================')
####Finishing Query 3###########################################################

####Starting Query 4###########################################################
print('=======================================================================')
print('Starting Query 4 - Quais os produtos mais vendidos nos últimos 5 anos?')
start = datetime.now()
#df4 = df.withColumn('data', f.to_date(df.data.cast(StringType()),'ddMMyyyy'))
df4 = df[(df['data']>pd.Timestamp(2018,1,1)) & (df['data']<pd.Timestamp(2022,12,4))]
df4.sort(df.quantidade.desc()).groupBy('produto', 'data', 'quantidade').count().show(20)
df4 = df4.select('produto', 'quantidade').describe().show(5000)
end = datetime.now()
print('The start execution time of Query 4 is:',start)
print('The end execution time of Query 4 is:',end)
print(f"Time taken in Query 4 (hh:mm:ss.ms) is {end - start}")
print('=======================================================================')
####Finishing Query 4###########################################################

####Starting Query 5###########################################################
print('=======================================================================')
print('Starting Query 5 - Quais os produtos menos vendidos nos últimos 5 anos?')
start = datetime.now()
df4 = df[(df['data']>pd.Timestamp(2018,1,1)) & (df['data']<pd.Timestamp(2022,12,4))]
df4.sort(df.quantidade.asc()).groupBy('produto', 'data', 'quantidade').count().show(20)
df4 = df4.select('produto', 'quantidade').describe().show(5000)
end = datetime.now()
print('The start execution time of Query 5 is:', start)
print('The end execution time of Query 5 is:', end)
print(f"Time taken in Query 5 (hh:mm:ss.ms) is {end - start}")
print('=======================================================================')
####Finishing Query 5###########################################################
print('')
print('')
end_global_time = datetime.now()
print(f"Time taken in GLOBAL PROGRAM (hh:mm:ss.ms) is {end_global_time - start_global_time}")

