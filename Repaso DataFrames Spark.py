# Databricks notebook source
# MAGIC %md
# MAGIC Las API de DataFrame se proporcionan mediante varias bibliotecas de procesamiento de datos, como Pandas en Python, Apache Spark y dplyr de R, cada una de las herramientas que ofrecen para controlar grandes conjuntos de datos con facilidad. Trabajar con DataFrames parece similar en todas las bibliotecas, pero cada biblioteca tiene algunas ligeras variaciones en sus funcionalidades.

# COMMAND ----------

#Ejemplo de DataFrame de spark en Python - Creación DataFrame sencillo
data = [("Alice", 34), ("Bob", 29), ("Charlie", 31)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

#Seleccionar filas
filtered_df = df.filter(df["age"] > 29)
df.filter(df["age"] > 29).show()

#Seleccionar columnas
df.select("name").show()

#Agrupado
df.groupBy("age").count().show()

# COMMAND ----------

#Filtrar DataFrame mediante consulta SQL
#Primero se debe crear una vista temporal
df.createOrReplaceTempView("people")
sql_df = spark.sql("SELECT name, age FROM people WHERE age > 29")


# COMMAND ----------

# MAGIC %md
# MAGIC Los DataFrames de Apache Spark son una abstracción basada en conjuntos de datos distribuidos resistentes (RDD). Spark DataFrame y Spark SQL usan un motor unificado de planificación y optimización, lo que le permite obtener un rendimiento casi idéntico en todos los lenguajes admitidos en Azure Databricks (Python, SQL, Scala y R).
