# Databricks notebook source
# MAGIC  %sh
# MAGIC  rm -r /dbfs/spark_lab
# MAGIC  mkdir /dbfs/spark_lab
# MAGIC  wget -O /dbfs/spark_lab/2019.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2019.csv
# MAGIC  wget -O /dbfs/spark_lab/2020.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2020.csv
# MAGIC  wget -O /dbfs/spark_lab/2021.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2021.csv

# COMMAND ----------


df = spark.read.load('spark_lab/*.csv', format='csv')
display(df.limit(100))

# COMMAND ----------

#Define  a schema for the dataframe
from pyspark.sql.types import *
from pyspark.sql.functions import *

orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
])

df = spark.read.load('/spark_lab/*.csv', format='csv', schema=orderSchema)
display(df.limit(100))

# COMMAND ----------

#Verirify data types
df.printSchema()

# COMMAND ----------

#Query data using Spark SQL
df.createOrReplaceTempView("salesorders")
spark_df = spark.sql("SELECT * FROM salesorders")
display(spark_df)

# COMMAND ----------

#View results as a visualization

%sql

SELECT * FROM salesorders

# COMMAND ----------

#Query as variable

sqlQuery = "SELECt CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
            FROM salesorders \
            GROUP BY CAST(YEAR(OrderDate) AS CHAT(4)) \
            ORDER BY OrderYear"

df_spark = spark.sql(sqlQuery)
df_spark.show()

# COMMAND ----------

#The matplotlib library requires a Pandas dataframe, so you need to convert the Spark dataframe returned by the Spark SQL query to a Pandas dataframe.

from matplotlib import pyplot as plt

#Marplotlib requires a Panda dataframe
df_sales = df_spark.toPandas()

#Create a bar plot
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

#Display plot
plt.show()

# COMMAND ----------


#Clean
plt.clf()

#Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

#Customize the chart
plt.title('Revenue by Year')
plt.xlabel('year')
plt.ylabel('Revenue')
plt.grid(color='#90a5e7', linestyle='--', linewidht=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

#Show the figure
plt.show()

# COMMAND ----------

#A figure can contain multiple subplots, each on its own axis.

# Clear the plot area
plt.clf()

# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))

# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')

# Create a pie chart of yearly order counts on the second axis
yearly_counts = df_sales['OrderYear'].value_counts()
ax[1].pie(yearly_counts)
ax[1].set_title('Orders per Year')
ax[1].legend(yearly_counts.keys().tolist())

# Add a title to the Figure
fig.suptitle('Sales Data')

# Show the figure
plt.show()

# COMMAND ----------

#Use the seaborn library

import seaborn as sns

#Clear the plot area
plt.clf()

#Cleate a bar chart
ax = sns.barplot(x="OrderYear", y="Grossrevenue", data=df_sales)
plt.show()

# COMMAND ----------

plt.clf()

#Set the visual theme for seaborn
sns.set_theme(style="whitegrid")

#Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()


# COMMAND ----------

#Lineplot
# Clear the plot area
plt.clf()
   
# Create a bar chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()
