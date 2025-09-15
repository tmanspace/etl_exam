from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType

def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-to-postgres") \
        .getOrCreate()

    schema = StructType() \
        .add("msno", StringType()) \
        .add("payment_method_id", IntegerType()) \
        .add("payment_plan_days", IntegerType()) \
        .add("plan_list_price", IntegerType()) \
        .add("actual_amount_paid", IntegerType()) \
        .add("is_auto_renew", BooleanType()) \
        .add("transaction_date", StringType()) \
        .add("membership_expire_date", StringType()) \
        .add("is_cancel", BooleanType())

    # === читаем поток из Kafka ===
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-2cciq5uvdtjsc608.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-transactions") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=\"admin1\" "
                "password=\"password1\";") \
        .option("startingOffsets", "earliest") \
        .load()

    # === парсим JSON в колонки ===
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("membership_expire_date", to_date(col("membership_expire_date"), "yyyy-MM-dd"))

    # === функция для записи batch в PostgreSQL ===
    def write_to_postgres(batch_df, batch_id):
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://rc1a-615ql8rrest6abl3.mdb.yandexcloud.net:6432/db1") \
            .option("dbtable", "transactions") \
            .option("user", "user1") \
            .option("password", "admin_pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    # === запускаем стрим ===
    query = parsed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "s3a://exam-bucket/kafka-checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
