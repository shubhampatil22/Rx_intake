from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


def main():
    spark = SparkSession.builder.getOrCreate()

    data = [
        (1, "Rahul", "rahul@test.com"),
        (2, "Amit", "amit@test.com"),
        (3, "Priya", "priya@test.com")
    ]

    columns = ["customer_id", "customer_name", "email"]

    df = spark.createDataFrame(data, columns)

    result_df = (
        df
        .withColumn("environment", lit("DATABRICKS"))
        .withColumn("processed_timestamp", current_timestamp())
    )

    result_df.write.format("delta").mode("overwrite").saveAsTable("customer_bronze")

    print("Customer ingestion completed successfully")


if __name__ == "__main__":
    main()
