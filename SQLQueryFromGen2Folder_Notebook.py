# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ###SQL Query From Azure Data Lake Gen 2 Files
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Data Lake Gen 2.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Connect to Storage Account

# COMMAND ----------

storage_account_name = "joelstorageaccount"
# comment
storage_account_access_key = "04z03QuvkTNGvGZNkO3DJolOSEMbtmUwhqyKRa9Qv9VUih5bKt3CVe80WwLqcRgOWlh4XKu8JkLVnEUipBegnw=="

# COMMAND ----------

# establishing an execution context
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Establish Folder Information

# COMMAND ----------

storage_type = "abfss:/"
file_container = "/tennis-center"


#file_folderlocation = "/transactions/2020/p04/wk01/dy6/"  #Filter data for 6th day of 2020-p04-wk01-dy6
#file_folderlocation = "/transactions/2020/p04/wk01/*/"    #Filter data for 1st wk of 2020-p04
#file_folderlocation = "/transactions/2020/p04/*/*/"  #Filter data for 4 period of 2020
file_folderlocation = "/transactions/2020/*/*/*/"  #Filter data for year 2020

file_type = "csv"
file_wildcard = "Sales*" # + file_type
file_loadpath = storage_type + file_container + "@" + storage_account_name + ".dfs.core.windows.net" + file_folderlocation + file_wildcard
file_loadpath

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Create Spark Data Frame

# COMMAND ----------

df = spark.read.format('csv').options(header='true', inferSchema='true').load(file_loadpath)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: Create View From Data Frame

# COMMAND ----------

df.createOrReplaceTempView("vTennisCenterSalesReport")

# COMMAND ----------

# MAGIC %md
# MAGIC  ### Step 5: Use SQL Language to Query View for "Detail Sales Transaction by Date"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM vTennisCenterSalesReport 
# MAGIC ORDER BY Date

# COMMAND ----------

# MAGIC %md
# MAGIC  ### Step 6: Use SQL Language to Query View for "Sum of Sales"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT sum(Sales) as SalesTotal 
# MAGIC FROM vTennisCenterSalesReport

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 7: Persist Table in Databricks Workspace

# COMMAND ----------

df.write.format("parquet").saveAsTable("TennisCenterSalesReport_Parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TennisCenterSalesReport_Parquet
