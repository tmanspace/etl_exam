from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.utils import AnalysisException


spark = SparkSession.builder.appName("Parquet ETL with Logging to S3").getOrCreate()

SOURCE_PATH = "s3a://exam-dataproc-result/transactions_v2.csv"
TARGET_PATH = "s3a://exam-bucket/transactions_v2_clean.parquet"

try:
    print(f"Reading: {SOURCE_PATH}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE_PATH)

    print("Scheme input:")
    df.printSchema()

    df = df.withColumn("actual_amount_paid", col("actual_amount_paid").cast(IntegerType())) \
           .withColumn("is_auto_renew", col("is_auto_renew").cast(BooleanType())) \
           .withColumn("is_cancel", col("is_cancel").cast(BooleanType())) \
           .withColumn("membership_expire_date", to_date(col("membership_expire_date").cast("string"), "yyyyMMdd")) \
           .withColumn("msno", col("msno").cast(StringType())) \
           .withColumn("payment_method_id", col("payment_method_id").cast(IntegerType())) \
           .withColumn("payment_plan_days", col("payment_plan_days").cast(IntegerType())) \
           .withColumn("plan_list_price", col("plan_list_price").cast(IntegerType())) \
           .withColumn("transaction_date", to_date(col("transaction_date").cast("string"),  "yyyyMMdd"))

    print("Scheme output:\n")
    df.printSchema()

    df = df.na.drop()

    print(f"Dropped NA\n Write to: {TARGET_PATH}\n")
    df.write.mode("overwrite").parquet(TARGET_PATH)

    print("Done")

except Exception as e:
    print("Error occured", e)

spark.stop()