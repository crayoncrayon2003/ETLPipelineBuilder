import os
import pandas as pd
from typing import Dict, Any, TYPE_CHECKING
import pluggy

from core.data_container.container import DataContainer
from core.infrastructure.storage_adapter import storage_adapter
from core.infrastructure.spark_session_factory import SparkSessionFactory
from core.plugin_manager.base_plugin import BasePlugin
from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

hookimpl = pluggy.HookimplMarker("etl_framework")

class SparkTransformer(BasePlugin):
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

    def run(self, input_data: DataContainer, container: DataContainer) -> DataContainer:
        input_path = self.params["input_path"]
        input_encoding = self.params.get("input_encoding", "utf-8")
        output_path = self.params["output_path"]
        query_file = self.params["query_file"]
        table_name = self.params.get("table_name", "source_data")
        use_spark_write = self.params.get("large_dataset", False)

        try:
            sql_query = self._get_query(query_file)
            logger.info(f"Loading input from: {input_path}")
            logger.info(f"Will write output to: {output_path}")

            spark = SparkSessionFactory.get_spark_session()

            pandas_df = storage_adapter.read_df(input_path, read_options={"encoding": input_encoding})
            spark_df = spark.createDataFrame(pandas_df)
            spark_df.createOrReplaceTempView(table_name)
            logger.info(f"Registered table '{table_name}' with {spark_df.count()} rows.")

            result_df = spark.sql(sql_query)
            logger.info(f"SQL executed. Result has {result_df.count()} rows.")

            if use_spark_write:
                logger.info("Using Spark native writer (large dataset mode).")
                storage_adapter.write_df(result_df, output_path, write_options={"spark": spark})
            else:
                logger.info("Using StorageAdapter via pandas DataFrame (small/medium dataset).")
                pandas_result = result_df.toPandas()
                storage_adapter.write_df(pandas_result, output_path)

        except Exception as e:
            raise RuntimeError(f"Error during Spark transformation: {e}")
        finally:
            SparkSessionFactory.stop_spark_session()
            logger.info("Spark session stopped if running locally.")

        return self.finalize_container(
            container,
            output_path=output_path,
            metadata={
                "input_path": input_path,
                "query_file": query_file,
                "table_name": table_name,
                "used_spark_write": use_spark_write
            }
        )
