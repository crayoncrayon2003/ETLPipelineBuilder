import os
import sys
import threading
from typing import Optional

from core.infrastructure.env_detector import is_running_on_aws


class SparkSessionFactory:
    _spark_session: Optional["object"] = None  # SparkSession or Glue SparkSession
    _glue_context: Optional["object"] = None   # GlueContext for AWS
    _lock: threading.Lock = threading.Lock()

    @staticmethod
    def get_spark_session():
        if SparkSessionFactory._spark_session is not None:
            return SparkSessionFactory._spark_session

        with SparkSessionFactory._lock:
            # ロック取得後に再チェック (double-checked locking)
            if SparkSessionFactory._spark_session is not None:
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

                os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
                os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

                conf = (
                    SparkConf()
                    .set("spark.executor.memory", "2g")
                    .set("spark.driver.memory", "2g")
                    .set("spark.executor.cores", "4")
                    .set("spark.sql.shuffle.partitions", "10")
                )

                spark = (
                    SparkSession.builder
                    .appName("ETLFrameworkSpark")
                    .config(conf=conf)
                    .getOrCreate()
                )

                SparkSessionFactory._spark_session = spark

        return SparkSessionFactory._spark_session

    @staticmethod
    def stop_spark_session():
        if is_running_on_aws():
            return

        with SparkSessionFactory._lock:
            if SparkSessionFactory._spark_session is not None:
                SparkSessionFactory._spark_session.stop()
                SparkSessionFactory._spark_session = None
                SparkSessionFactory._glue_context = None

    @staticmethod
    def get_glue_context():
        # ローカル(Windows)環境で呼ばれた場合に明確なエラーを出す
        if not is_running_on_aws():
            raise RuntimeError(
                "GlueContext is only available on AWS Glue. "
                "get_glue_context() cannot be called in a local environment."
            )

        if SparkSessionFactory._glue_context is None:
            SparkSessionFactory.get_spark_session()

        return SparkSessionFactory._glue_context