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
select DISTINCT(Model_ID) as Model_ID,model_category from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

display(df_source)

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
      df_sink=spark.sql('''
            select dim_model_key,Model_ID,model_category
            from cars_catalog.gold.dim_model           
      ''')
else:
      df_sink=spark.sql('''
            select 1 as dim_model_key,Model_ID,model_category
            from parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`
            where 1=0
      ''')

# COMMAND ----------

display(df_sink)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering new records and old records

# COMMAND ----------

df_filter=df_source.join(df_sink,df_source.Model_ID==df_sink.Model_ID,'left').select(df_source.Model_ID,df_source.model_category,df_sink.dim_model_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC df_filter_old

# COMMAND ----------

df_filter_old=df_filter.filter(col("dim_model_key").isNotNull())

# COMMAND ----------

display(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_new**

# COMMAND ----------

df_filter_new=df_filter.filter(col("dim_model_key").isNull()).select(df_source["Model_ID"],df_source["model_category"])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC fetch the max surrogate key from existing table

# COMMAND ----------

if not spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_value=max_value_df.collect()[0][0]+1


# COMMAND ----------

# MAGIC %md
# MAGIC **create surrogate key column and add the max surrogate key**

# COMMAND ----------

df_filter_new=df_filter_new.withColumn("dim_model_key",max_value+monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl=DeltaTable.forPath(spark, "abfss://gold@dlcarproject.dfs.core.windows.net/dim_model")
    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_model_key = src.dim_model_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
#Initial run
else:
    df_final.write.format("delta").mode("overwrite")\
        .option("path","abfss://gold@dlcarproject.dfs.core.windows.net/dim_model").saveAsTable("cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

