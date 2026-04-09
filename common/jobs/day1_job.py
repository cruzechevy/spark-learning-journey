from pyspark.sql import SparkSession


def load_data(spark):
    df_tele = spark.read.csv("/opt/spark-data/telematics.csv", header=True, inferSchema=True)
    df_vehicle = spark.read.csv("/opt/spark-data/vehicles.csv", header=True, inferSchema=True)
    return df_tele, df_vehicle


def transform(df_tele, df_vehicle):
    return df_tele.filter(df_tele["speed"] > 60) \
        .join(df_vehicle, "vehicle_id") \
        .groupBy("model") \
        .avg("speed")


def main():
    spark = SparkSession.builder \
        .appName("Day1_Telematics_Job") \
        .getOrCreate()

    df_tele, df_vehicle = load_data(spark)
    result = transform(df_tele, df_vehicle)

    result.show()

    spark.stop()


if __name__ == "__main__":
    main()