# Databricks notebook source
# MAGIC %md
# MAGIC ## Creating Fact **Table**

# COMMAND ----------

# MAGIC %md
# MAGIC ** reading silver data**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_silver = spark.sql("select * from parquet.`abfss://silver@salesstorageacc.dfs.core.windows.net/carsales`")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading all Dimensions

# COMMAND ----------

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")
df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")
df_date   = spark.sql("select * from cars_catalog.gold.dim_date")
df_model = spark.sql("select * from cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %md
# MAGIC **Bringing Keys to fact table**

# COMMAND ----------

df_fact = df_silver.join(df_dealer, df_silver.DEALER_ID == df_dealer.Dealer_ID, 'left')\
                    .join(df_branch, df_silver.BRANCH_ID == df_branch.Branch_ID, 'left')\
                    .join(df_date, df_silver.DATE_ID == df_date.date_id, 'left')\
                    .join(df_model, df_silver.MODEL_ID == df_model.model_id, 'left')\
                    .select(df_silver.REVENUE, df_silver.UNITS_SOLD, df_silver.rev_per_unit, df_branch.dim_branch_key , df_dealer.dim_dealer_key,df_model.dim_model_key, df_date.dim_date_key)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.silver.factsales"):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@salesstorageacc.dfs.core.windows.net/factsales")
    delta_tbl.alias("target")\
    .merge(df_fact.alias("source"), "target.dim_branch_key = source.dim_branch_key and target.dim_dealer_key = source.dim_dealer_key and target.dim_model_key = source.dim_model_key and target.dim_date_key = source.dim_date_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact.write.format('delta')\
            .mode('overwrite')\
            .option('path', "abfss://gold@salesstorageacc.dfs.core.windows.net/factsales")\
            .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales;