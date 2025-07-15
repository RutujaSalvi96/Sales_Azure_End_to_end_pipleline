# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
        .option('inferschema', True)\
        .load('abfss://bronze@salesstorageacc.dfs.core.windows.net/rawdata/')


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Trasformation

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('Model_Category', split(col('MODEL_ID'), '-')[0])
df.display()

# COMMAND ----------

#typecasting Unit_sold to string
df.withColumn('units_sold', col('units_sold').cast('string')).printSchema()

# COMMAND ----------

#arthemetic operation 

df = df.withColumn('rev_per_unit', col('revenue')/col('units_sold'))
df.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC - # Ad-hoc Analysis

# COMMAND ----------

df.createOrReplaceTempView('v1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select year,branchname, sum(Units_sold) as unit_count from v1
# MAGIC  group by year,branchname
# MAGIC  order by year, unit_count desc

# COMMAND ----------

df.groupBy('YEAR', 'BranchName').agg(sum('UNITS_SOLD').alias('sum_unit_sold')).sort('year','sum_unit_sold', ascending=[1,0]).display()

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path',"abfss://silver@salesstorageacc.dfs.core.windows.net/carsales").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@salesstorageacc.dfs.core.windows.net/carsales`