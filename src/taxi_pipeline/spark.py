"""Spark local execution and Hadoop S3A configuration."""
from pyspark.sql import SparkSession


def spark_session(settings):
    spark = (
        SparkSession.builder
        .appName("artefact-chicago-taxi")
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.session.timeZone", "America/Chicago")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def s3_path(settings, key):
    return f"s3a://{settings.bucket}/{key}"
