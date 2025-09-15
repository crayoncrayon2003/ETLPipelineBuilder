import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(script_dir, "scripts")
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

from core.data_container.container import DataContainer
from core.pipeline.step_executor import StepExecutor

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)


def ensure_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def main():
    logger.info("--- Starting direct pipeline execution ---")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    working_dir = os.path.abspath(os.path.join(script_dir, "..", "test", "data"))

    http_output_file = os.path.join(working_dir, "Step1", "device_data.csv")
    duckdb_output_file = os.path.join(working_dir, "Step3", "run_pipeline_directly.parquet")
    jinja2_output_file = os.path.join(working_dir, "Step5", "run_pipeline_directly.json")

    sql_file = os.path.join(working_dir, "Step2", "step2_with_spark.sql")
    j2_template_file = os.path.join(working_dir, "Step4", "step4.j2")

    ensure_dir_exists(os.path.dirname(http_output_file))
    ensure_dir_exists(os.path.dirname(duckdb_output_file))
    ensure_dir_exists(os.path.dirname(jinja2_output_file))

    step_executor = StepExecutor()

    try:
        logger.info("[Step 1: from_http]")
        http_params = {
            "url": "http://localhost:8080/device_data.csv",
            "output_path": http_output_file
        }
        http_step_config = {
            "name": "step1_from_http",
            "plugin": "from_http",
            "params": http_params
        }
        http_result_container = step_executor.execute_step(http_step_config)

        logger.info("[Step 2: with_duckdb]")
        duckdb_params = {
            "input_path": http_result_container.get_primary_file_path(),
            "output_path": duckdb_output_file,
            "query_file": sql_file,
            "table_name": "source_data"
        }
        duckdb_step_config = {
            "name": "step2_with_spark",
            "plugin": "with_spark",
            "params": duckdb_params
        }
        duckdb_result_container = step_executor.execute_step(
            duckdb_step_config,
            inputs={"input_data": http_result_container}
        )

        logger.info("[Step 3: with_jinja2]")
        jinja2_params = {
            "input_path": duckdb_result_container.get_primary_file_path(),
            "output_path": jinja2_output_file,
            "template_path": j2_template_file
        }
        jinja2_step_config = {
            "name": "step3_with_jinja2",
            "plugin": "with_jinja2",
            "params": jinja2_params
        }
        jinja2_result_container = step_executor.execute_step(
            jinja2_step_config,
            inputs={"input_data": duckdb_result_container}
        )

        logger.info("--- Pipeline execution finished successfully! ---")

    except Exception:
        logger.error("\n--- An error occurred during pipeline execution ---")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
