# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "a162921f-177b-41d5-9d1c-627b13353ede",
"fs.azure.account.oauth2.client.secret": 'ZYj8Q~v~dKc2BIMSQ3Gp38N50Dj96Iz6oloM9bUe',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/fe93855b-8edc-48ee-a63f-87dc6435ba6d/oauth2/token"}

# Step 1: Set Spark config for authentication (using Storage Account Key)
spark.conf.set(
  "fs.azure.account.key.tokyoolympicgaurav.dfs.core.windows.net",
  "aBQFKEemu6WftdSY9Onz7JTSGeH5MaBUlsNIZeJ5QCFAxoaBQralpkhs/dNhYBE63mkt/j9gBEng+AStGeEmbw=="  # Replace with real key from Azure portal
)

# Step 2: Define the folder path (not just one file)
base_path = "abfss://tokyo-olympic-gaurav@tokyoolympicgaurav.dfs.core.windows.net/"
raw_path = base_path + "raw-data/"
transformed_path = base_path + "transformed-data/"

# Step 3: Read all CSV files in the folder into one DataFrame (schemas must match)
df_all = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(base_path)

# Step 4: Load all CSVs as separate DataFrames
athletes = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path + "athletes.csv")
coaches = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path + "coaches.csv")
entriesgender = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path + "entriesgender.csv")
medals = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path + "medals.csv")
teams = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path + "teams.csv")
  

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# Calculate average gender distribution
average_entries_by_gender = entriesgender.withColumn("Avg_Female", col("Female") / col("Total")) \
                                         .withColumn("Avg_Male", col("Male") / col("Total"))

average_entries_by_gender.show(5)

# COMMAND ----------

# Find top countries by gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry", "Gold")

top_gold_medal_countries.show(5)

# COMMAND ----------

# Step 5: Write all transformed DataFrames to Azure container
# ------------------------------------------

athletes.repartition(1).write.mode("overwrite").option("header", "true").csv(transformed_path + "athletes")
print("✅ Athletes saved")

coaches.repartition(1).write.mode("overwrite").option("header", "true").csv(transformed_path + "coaches")
print("✅ Coaches saved")

entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv(transformed_path + "entriesgender")
print("✅ EntriesGender saved")

medals.repartition(1).write.mode("overwrite").option("header", "true").csv(transformed_path + "medals")
print("✅ Medals saved")

teams.repartition(1).write.mode("overwrite").option("header", "true").csv(transformed_path + "teams")
print("✅ Teams saved")

# ------------------------------------------
# Step 6 (Optional): Verify in Azure or via code
# ------------------------------------------
display(dbutils.fs.ls(transformed_path))  # This lists all transformed directories