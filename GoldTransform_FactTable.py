# Databricks notebook source
# MAGIC %md
# MAGIC ##Creating Fact table for the data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fetching the table from Silver layer

# COMMAND ----------

df_Source = spark.sql('''Select * from parquet.`abfss://silver@cardemodatalake.dfs.core.windows.net/CarSalesData`''')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create df for each dimensions

# COMMAND ----------

df_model = spark.sql("Select * from cars_catalog.gold.dim_model")
df_dealer = spark.sql("Select * from cars_catalog.gold.dim_dealer")
df_date = spark.sql("Select * from cars_catalog.gold.dim_date")
df_branch = spark.sql("Select * from cars_catalog.gold.dim_branch")

# COMMAND ----------

display(df_Source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Join to bring the Dimension Keys and fact table

# COMMAND ----------

df_fact = df_Source.join(df_model, df_Source['Model_ID'] == df_model['ModelID'], how='left')\
    .join(df_dealer, df_Source['Dealer_ID'] == df_dealer['DealerID'], how='left')\
        .join(df_date, df_Source['Date_ID'] == df_date['DateID'], how='left')\
            .join(df_branch, df_Source['Branch_ID'] == df_branch['BranchID'], how='left')\
                .select(df_Source['Revenue'],df_Source['Units_Sold'],df_Source['PerUnit_Cost'],df_model['dim_Mod_Key'],df_dealer['dim_Dealer_Key'],df_date['dim_Date_Key'],df_branch['dim_Branch_Key'])

# COMMAND ----------

display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the Fact table or Save Gold transform

# COMMAND ----------

if spark.catalog.tableExists('GoldTable'):
    deltaTable = DeltaTable.forName(spark, "cars_catalog.gold.GoldTable")
    deltaTable.alias("target").merge(df_fact.alias("source"), "target.dim_Mod_Key = source.dim_Mod_Key and target.dim_Dealer_Key = source.dim_Dealer_Key and target.dim_Date_Key = source.dim_Date_Key and target.dim_Branch_Key = source.dim_Branch_Key")\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll().execute()
else :
    df_fact.write.format("delta").mode("overwrite").option("path","abfss://gold@cardemodatalake.dfs.core.windows.net/GoldTable").saveAsTable('cars_catalog.gold.GoldTable')

# COMMAND ----------

display(spark.sql("Select * from cars_catalog.gold.GoldTable"))

# COMMAND ----------

