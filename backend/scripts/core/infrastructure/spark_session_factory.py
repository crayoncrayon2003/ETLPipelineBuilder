import os
from typing import Optional
from core.infrastructure.env_detector import is_running_on_aws

class SparkSessionFactory:
    _spark_session: Optional["object"] = None  # SparkSession or Glue SparkSession
    _glue_context: Optional["object"] = None   # GlueContext for AWS

    @staticmethod
    def get_spark_session():
        if SparkSessionFactory._spark_session:
            return SparkSessionFactory._spark_session

        if is_running_on_aws():
            from pyspark.context import SparkContext
            from awsglue.context import GlueContext

            sc = SparkContext.getOrCreate()
            glue_context = GlueContext(sc)
            SparkSessionFactory._glue_context = glue_context
            SparkSessionFactory._spark_session = glue_context.spark_session

        else:
            from pyspark import SparkConf
            from pyspark.sql import SparkSession

            PYTHON_PATH = "/usr/bin/python3.9"
            os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
            os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH
            os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

            conf = SparkConf() \
                .set("spark.executor.memory", "2g") \
                .set("spark.driver.memory", "2g") \
                .set("spark.executor.cores", "4") \
                .set("spark.sql.shuffle.partitions", "10")

            spark = SparkSession.builder \
                .appName("ETLFrameworkSpark") \
                .config(conf=conf) \
                .getOrCreate()

            SparkSessionFactory._spark_session = spark

        return SparkSessionFactory._spark_session

    @staticmethod
    def stop_spark_session():
        if SparkSessionFactory._spark_session and not is_running_on_aws():
            SparkSessionFactory._spark_session.stop()
            SparkSessionFactory._spark_session = None

    @staticmethod
    def get_glue_context():
        if not SparkSessionFactory._glue_context:
            SparkSessionFactory.get_spark_session()
        return SparkSessionFactory._glue_context
