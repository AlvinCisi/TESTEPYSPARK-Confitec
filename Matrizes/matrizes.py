import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import os

os.environ['PYSPARK_PYTHON'] = 'C:\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\Python311\python.exe'

# Cria uma sessão Spark
spark = SparkSession.builder.appName("random_matrix").getOrCreate()

# Cria uma matriz aleatória usando numpy
rand_matrix_a = np.random.randint(low=1, high=101, size=(4,4))
rand_matrix_b = np.random.randint(low=1, high=101, size=(4,4))

# Cria um RDD a partir da matriz e converte para um DataFrame
rdd_a = spark.sparkContext.parallelize(rand_matrix_a.tolist())
df_a = rdd_a.map(lambda x: tuple(x)).toDF()

rdd_b = spark.sparkContext.parallelize(rand_matrix_b.tolist())
df_b = rdd_b.map(lambda x: tuple(x)).toDF()

# Exibe o DataFrame
print("Matriz A:")
df_a.show()

print("Matriz B:")
df_b.show()

# Multiplica as matrizes A e B e exibe a matriz Produto
rdd_product = df_a.rdd.zip(df_b.rdd).map(lambda x: [a*b for a, b in zip(x[0], x[1])])
df_product = rdd_product.toDF()

# exibe o resultado
print("Matriz Produto:")
df_product.show()

# Encerra a sessão no Spark
spark.stop()