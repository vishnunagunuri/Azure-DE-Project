# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact Table

# COMMAND ----------

# MAGIC %md
# MAGIC ***Reading Silver Data***

# COMMAND ----------

df_silver=spark.sql("SELECT * FROM parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Reading all Dims

# COMMAND ----------

df_dealer=spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")
df_date=spark.sql("SELECT * FROM cars_catalog.gold.dim_date")
df_branch=spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")
df_model=spark.sql("SELECT * FROM cars_catalog.gold.dim_model")


# COMMAND ----------

# MAGIC %md
# MAGIC Bringing keys to the Fact Table

# COMMAND ----------

df_fact=df_silver.join(df_branch,"Branch_ID","left")\
                .join(df_dealer,"Dealer_ID","left")\
                .join(df_date,"Date_ID","left")\
                .join(df_model,"Model_ID","left")\
                .select(df_silver.Revenue,df_silver.Units_Sold,df_silver.RevPerUnit,df_branch.dim_branch_key,df_dealer.dim_dealer_key,df_date.dim_date_key,df_model.dim_model_key)

# COMMAND ----------

display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC writing fact table
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.fact_sales"):
    delta_tbl=DeltaTable.forName(spark,"cars_catalog.gold.fact_sales")
    delta_tbl.alias("trg").merge(df_fact.alias("src"),"trg.dim_branch_key=src.dim_branch_key AND trg.dim_dealer_key=src.dim_dealer_key AND trg.dim_date_key=src.dim_date_key AND trg.dim_model_key=src.dim_model_key")\
           .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    df_fact.write.format("delta")\
                .mode("overwrite")\
                .option("path","abfss://gold@dlcarproject.dfs.core.windows.net/fact_sales")\
                .saveAsTable("cars_catalog.gold.fact_sales")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.fact_sales

# COMMAND ----------

