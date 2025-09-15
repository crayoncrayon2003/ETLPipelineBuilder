import os
import pandas as pd
from typing import Dict, Any, Optional
import pluggy
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from core.data_container.container import DataContainer
from utils.logger import setup_logger
from core.infrastructure.storage_adapter import storage_adapter

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

hookimpl = pluggy.HookimplMarker("etl_framework")


class SparkTransformer:
    """
    Transforms data using a SQL query powered by Spark.
    All file I/O is handled via StorageAdapter.
    """

    @hookimpl
    def get_plugin_name(self) -> str:
        return "with_spark"

    @hookimpl
    def get_parameters_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "input_path": {"type": "string", "title": "Input File Path"},
                "input_encoding": {"type": "string", "title": "Input File Encoding", "default": "utf-8"},
                "output_path": {"type": "string", "title": "Output File Path"},
                "query_file": {"type": "string", "title": "SQL Query File Path"},
                "table_name": {"type": "string", "title": "Table Name for Input", "default": "source_data"},
                "large_dataset": {
                    "type": "boolean",
                    "title": "Use Spark Write for Large Datasets",
                    "default": False
                }
            },
            "required": ["input_path", "output_path", "query_file"]
        }

    def _get_query(self, path: str) -> str:
        return storage_adapter.read_text(path)

    @hookimpl
    def execute_plugin(
        self, params: Dict[str, Any], inputs: Dict[str, Optional[DataContainer]]
    ) -> Optional[DataContainer]:

        input_path = params["input_path"]
        input_encoding = params.get("input_encoding", "utf-8")
        output_path = params["output_path"]
        query_file = params["query_file"]
        table_name = params.get("table_name", "source_data")
        use_spark_write = params.get("large_dataset", False)

        sql_query = self._get_query(query_file)

        logger.info(f"Loading input from: {input_path}")
        logger.info(f"Will write output to: {output_path}")


        try:
            # ###########################
            # setting spark
            os.environ["PYSPARK_PYTHON"] = "python3"
            os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
            os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

            conf = SparkConf() \
                    .setAppName("ETLFrameworkSpark") \
                    .set("spark.pyspark.driver.python", "/usr/bin/python3.9") \
                    .set("spark.pyspark.python", "/usr/bin/python3.9") \
                    .set("spark.executor.memory", "2g") \
                    .set("spark.driver.memory", "2g") \
                    .set("spark.executor.cores", "4")

            spark = SparkSession \
                .builder \
                .config(conf=conf) \
                .getOrCreate()

            # Read input via StorageAdapter (pandas DataFrame)
            pandas_df = storage_adapter.read_df(input_path, read_options={"encoding": input_encoding})

            # Convert to Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df)
            spark_df.createOrReplaceTempView(table_name)
            logger.info(f"Registered table '{table_name}' with {spark_df.count()} rows.")

            # Execute SQL
            result_df: SparkDataFrame = spark.sql(sql_query)
            logger.info(f"SQL executed. Result has {result_df.count()} rows.")

            # Write result
            if use_spark_write:
                logger.info("Using Spark native writer (large dataset mode).")
                storage_adapter.write_df(result_df, output_path, write_options={"spark": spark})
            else:
                logger.info("Using StorageAdapter via pandas DataFrame (small/medium dataset).")
                pandas_result = result_df.toPandas()
                storage_adapter.write_df(pandas_result, output_path)

        except Exception as e:
            logger.error(f"Error during Spark transformation: {e}", exc_info=True)
            raise
        finally:
            spark.stop()
            logger.info("Spark session stopped.")

        output_container = DataContainer()
        output_container.add_file_path(output_path)
        return output_container
