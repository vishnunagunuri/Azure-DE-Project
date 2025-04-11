# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df=spark.read.format("parquet").option("inferschema",True).load("abfss://bronze@dlcarproject.dfs.core.windows.net/raw_data")

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.filter(df.Date_ID=='DT01246'))

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformation

# COMMAND ----------

from pyspark.sql.functions import col,split
df_col=df.withColumn("Model",split(col("Model_ID"),"-")[0])

# COMMAND ----------

display(df_col)

# COMMAND ----------

df_renamed=df.withColumn("Model_Category",col("Model_ID"))

# COMMAND ----------

display(df_renamed)

# COMMAND ----------

from pyspark.sql.functions import cast
df_casted=df_renamed.withColumn("Year",col("Year").cast("string"))

# COMMAND ----------

df_casted.printSchema()

# COMMAND ----------

df=df_renamed.withColumn("RevPerUnit", col("Revenue") / col("Units_Sold"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ad-hoc analysis

# COMMAND ----------

#units sold by each branch each year
from pyspark.sql.functions import sum,desc,col
display(df.groupBy("BranchName","Year").agg(sum("Units_Sold").alias("Total_Units_Sold")).sort(col("Year"),desc(col("Total_Units_Sold"))))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing data to silver table

# COMMAND ----------

df.write.format("parquet").mode("overwrite").save("abfss://silver@dlcarproject.dfs.core.windows.net/carsales")

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://silver@dlcarproject.dfs.core.windows.net/carsales`

# COMMAND ----------



# COMMAND ----------

