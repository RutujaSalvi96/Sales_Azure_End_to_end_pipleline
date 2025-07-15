# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Flag parameter 

# COMMAND ----------

dbutils.widgets.text("incremental_flag", "0")

# COMMAND ----------

inc_flag = dbutils.widgets.get("incremental_flag")
print(inc_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taking Relevant Columns

# COMMAND ----------

df_src = spark.sql('''
select distinct(Dealer_ID) as Dealer_ID, DealerName from parquet.`abfss://silver@salesstorageacc.dfs.core.windows.net/carsales`
''')
df_src.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # dim_model sink Inital and Incrmental Load 

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

    df_sink = spark.sql('''
                        select 1 as dim_dealer_key,  Dealer_ID, DealerName
                        from parquet.`abfss://silver@salesstorageacc.dfs.core.windows.net/carsales` 
                        where 1=0
                        ''')
else:
    df_sink = spark.sql('''
                        select dim_dealer_key,  Dealer_ID, DealerName
                        from cars_catalog.gold.dim_dealer
                        ''')


# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering old records and new records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_ID' ]== df_sink['Dealer_ID'], "left").select(df_src['Dealer_ID'], df_src['DealerName'],df_sink['dim_dealer_key'])
df_filter.display() 

# COMMAND ----------

df_old = df_filter.filter(df_filter['dim_dealer_key'].isNotNull())
df_old.display() 

# COMMAND ----------

df_new = df_filter.filter(df_filter['dim_dealer_key'].isNull()).select("Dealer_ID", "DealerName")
df_new.display()
   

# COMMAND ----------

# MAGIC %md
# MAGIC # **Create Surogate Key**

# COMMAND ----------

#fetch max souragate key
if ( inc_flag == "0" ):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]+ 1

# COMMAND ----------

#creating surogate key column and the max surugate key

df_new = df_new.withColumn('dim_dealer_key', max_value+monotonically_increasing_id())

df_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC **creating **Final** DF**

# COMMAND ----------

df_final = df_old.union(df_new) 

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#incremental run
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    #here we are creating a object of the delta table as a target table
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@salesstorageacc.dfs.core.windows.net/dim_dealer")
    delta_tbl.alias("target")\
        .merge(df_final.alias("source"), "target.dim_dealer_key = source.dim_dealer_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

#Inital run
else:
    df_final.write.mode("append")\
        .format("delta")\
        .option("path","abfss://gold@salesstorageacc.dfs.core.windows.net/dim_dealer")\
        .saveAsTable("cars_catalog.gold.dim_dealer"   )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer;