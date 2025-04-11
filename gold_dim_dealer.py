# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FLAG PARAMATER

# COMMAND ----------

# MAGIC %md
# MAGIC FLag parameter is a flag which tells that this is our initial run or incremental run

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("incremental flag","0")

# COMMAND ----------

incremental_flag=dbutils.widgets.get("incremental flag")
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dimension Model

# COMMAND ----------

df_source = spark.sql('''
select Distinct(Dealer_ID) as dealer_id,DealerName from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

display(df_source)

# COMMAND ----------

# MAGIC %md
# MAGIC dim_branch sink:Initial and incremental(just bring the schema if table Not Exists)

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
      df_sink=spark.sql('''
            select dim_dealer_key,Dealer_ID,DealerName
            from cars_catalog.gold.dim_dealer            
      ''')
else:
      df_sink=spark.sql('''
            select 1 as dim_dealer_key,Dealer_ID,DealerName
            from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
            where 1=0
      ''')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering new records and old records

# COMMAND ----------

df_filter=df_source.join(df_sink,df_source.dealer_id==df_sink.Dealer_ID,'left').select(df_source.dealer_id,df_source.DealerName,df_sink.dim_dealer_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_dealer_key").isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_dealer_key").isNull()).select(df_source["Dealer_ID"],df_source["DealerName"])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC fetch the max surrogate key from existing table

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value=max_value_df.collect()[0][0]+1


# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_dealer_key",max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create final df

# COMMAND ----------

df_final=df_filter_old.union(df_filter_new)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD TYPE 1-UPSERT

# COMMAND ----------

#Incremental_run
from delta.tables import DeltaTable
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl=DeltaTable.forPath(spark, "abfss://gold@dlcarproject.dfs.core.windows.net/dim_dealer")
    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_dealer_key = src.dim_dealer_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#Initial run
else:
    df_final.write.format("delta").mode("overwrite")\
        .option("path","abfss://gold@dlcarproject.dfs.core.windows.net/dim_dealer").saveAsTable("cars_catalog.gold.dim_dealer")