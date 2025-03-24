# Databricks notebook source
# MAGIC %md
# MAGIC ### Data ingestion into Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Pyspark API

# COMMAND ----------

df = spark.read.format('parquet')\
    .option('inferSchema',True)\
    .load('abfss://bronze@cardemodatalake.dfs.core.windows.net/RawData')
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transform - Split Model Name

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('Model_Name',split(col('Model_ID'), '-')[0])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transform - Calculate Per Unit Cost

# COMMAND ----------

df = df.withColumn('PerUnit_Cost',col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transform - AD-HOC and Sales

# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the Transform Data to Silver Layer

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','abfss://silver@cardemodatalake.dfs.core.windows.net/CarSalesData').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the Saved data in Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@cardemodatalake.dfs.core.windows.net/CarSalesData`

# COMMAND ----------

