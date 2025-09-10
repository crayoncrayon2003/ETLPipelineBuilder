import sys
import os
from pathlib import Path

scripts_path = Path(__file__).resolve().parent / "scripts"
if str(scripts_path) not in sys.path:
    sys.path.append(str(scripts_path))

from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

from utils.logger import setup_logger

log_level = os.getenv("LOG_LEVEL", "INFO")
logger = setup_logger(__name__, level=log_level)

def main():
    logger.info("--- Starting direct pipeline execution ---")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    working_dir = os.path.join(script_dir, "..", "test", "data")
    working_dir = os.path.abspath(working_dir)
    working_dir = Path(working_dir)

    http_output_file = working_dir / "Step1" / "device_data.csv"
    duckdb_output_file = working_dir / "Step3" / "run_pipeline_directly.parquet"
    jinja2_output_file = working_dir / "Step5" / "run_pipeline_directly.json"

    sql_file = working_dir / "Step2" / "step2.sql"
    j2_template_file = working_dir / "Step4" / "step4.j2"

    http_output_file.parent.mkdir(parents=True, exist_ok=True)
    duckdb_output_file.parent.mkdir(parents=True, exist_ok=True)
    jinja2_output_file.parent.mkdir(parents=True, exist_ok=True)


    try:
        logger.info("[Step 1: from_http]")
        http_params = {
            "url": "http://localhost:8080/device_data.csv",
            "output_path": http_output_file
        }
        http_result_container = framework_manager.call_plugin_execute(
            plugin_name="from_http",
            params=http_params,
            inputs={}
        )


        logger.info("[Step 2: with_duckdb]")
        duckdb_params = {
            "input_path": http_result_container.get_primary_file_path(),
            "output_path": duckdb_output_file,
            "query_file": sql_file,
            "table_name": "source_data"
        }
        duckdb_result_container = framework_manager.call_plugin_execute(
            plugin_name="with_duckdb",
            params=duckdb_params,
            inputs={"input_data": http_result_container}
        )


        logger.info("[Step 3: with_jinja2]")
        jinja2_params = {
            "input_path": duckdb_result_container.get_primary_file_path(),
            "output_path": jinja2_output_file,
            "template_path": j2_template_file
        }
        jinja2_result_container = framework_manager.call_plugin_execute(
            plugin_name="with_jinja2",
            params=jinja2_params,
            inputs={"input_data": duckdb_result_container}
        )

        logger.info("--- Pipeline execution finished successfully! ---")

    except Exception as e:
        logger.error(f"\n--- An error occurred during pipeline execution ---")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()