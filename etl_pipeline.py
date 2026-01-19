from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session with BigQuery support
spark = SparkSession.builder.appName("SimpleETL").getOrCreate()

# 1️⃣ Read CSV from GCS
gcs_path = "gs://my-data-bucket1111/sample_sales_data.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(gcs_path)

df.show(5)

# 2️ Handle missing values
df_clean = df.dropna(subset=["order_id", "customer_id", "amount"])
df_clean = df_clean.fillna({"product": "Unknown", "region": "Unknown"})

# 3️⃣ Remove duplicates
df_clean = df_clean.dropDuplicates(["order_id"])

# 4️⃣ Optional transformation
df_clean = df_clean.withColumn("amount_usd", col("amount") * 1.0)

# 5️⃣ Write to BigQuery
project_id = "dataproc-pipeline"
dataset_id = "my_dataset"
table_id = "sales_cleaned"

df_clean.write.format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.{table_id}") \
    .option("temporaryGcsBucket", "my-data-bucket1111") \
    .mode("overwrite") \
    .save()

print("Data successfully loaded to BigQuery")
