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
select Distinct(Date_ID) as Date_ID from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

display(df_source)

# COMMAND ----------

# MAGIC %md
# MAGIC dim_branch sink:Initial and incremental(just bring the schema if table Not Exists)

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
      df_sink=spark.sql('''
            select dim_date_key,Date_ID
            from cars_catalog.gold.dim_date            
      ''')
else:
      df_sink=spark.sql('''
            select 1 as dim_date_key,Date_ID
            from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
            where 1=0
      ''')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering new records and old records

# COMMAND ----------

df_filter=df_source.join(df_sink,df_source.Date_ID==df_sink.Date_ID,'left').select(df_source.Date_ID,df_sink.dim_date_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_date_key").isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_date_key").isNull()).select(df_source["Date_ID"])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC fetch the max surrogate key from existing table

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_date_key) from cars_catalog.gold.dim_date")
    max_value=max_value_df.collect()[0][0]+1


# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_date_key",max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_tbl=DeltaTable.forPath(spark, "abfss://gold@dlcarproject.dfs.core.windows.net/dim_date")
    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_date_key = src.dim_date_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#Initial run
else:
    df_final.write.format("delta").mode("overwrite")\
        .option("path","abfss://gold@dlcarproject.dfs.core.windows.net/dim_date").saveAsTable("cars_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date

# COMMAND ----------

