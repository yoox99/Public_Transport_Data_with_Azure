# Databricks notebook source
#Configure Spark to access Azure Data Lake Storage Gen2 using the account key
spark.conf.set(
    f"fs.azure.account.key.yasseraithninistorage.dfs.core.windows.net", 
    "t2QZGycU0Ye2V8Ne6ljeGAMXCj/kQSmgMQpUW75D6oFr7TSM3O/BHnz1bJA+VgCAIW5e0U7qrXQh+ASt/prQsw=="
)              

# List the contents of the 'raw' folder in Azure Data Lake Storage Gen2
dbutils.fs.ls("abfss://publictransportdata@yasseraithninistorage.dfs.core.windows.net/public_transport_data/processed/")

#Set data lake file location
file_location = "abfss://publictransportdata@yasseraithninistorage.dfs.core.windows.net/public_transport_data/processed/processedTransportDataOf_rawTransportDataOf_01_2023.csv.csv"
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "True").option("header",
"True").option("delimeter",",").load(file_location)

# COMMAND ----------

#display the dataframe
display(df)

# COMMAND ----------

df.createOrReplaceTempView("TransportPublic")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TransportPublic LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TransportPublic WHERE Date = '2023-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DelayCategory, COUNT(*) AS Count
# MAGIC FROM TransportPublic
# MAGIC GROUP BY DelayCategory

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT TransportType, ROUND(AVG(passengers)) AS AvgPassengers
# MAGIC FROM TransportPublic
# MAGIC GROUP BY TransportType
