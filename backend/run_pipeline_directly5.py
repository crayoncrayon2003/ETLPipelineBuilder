import sys
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(script_dir, "scripts")
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

from utils.logger import AppLogger, setup_logger

applogger = AppLogger(inputdataname="MainModule")
applogger.init_logger("INFO")
logger = setup_logger(__name__)

from core.data_container.container import DataContainer, DataContainerStatus
from core.pipeline.step_executor import StepExecutor
from core.infrastructure.storage_path_utils import is_memory_path


def ensure_dir_exists(path):
    # memory:// パスはメモリ上で管理するためディレクトリ作成不要
    if is_memory_path(path):
        return
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def main():
    logger.info("--- Starting direct pipeline execution ---")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    working_dir = os.path.abspath(os.path.join(script_dir, "..", "test", "data"))

    http_output_file   = "memory://run_pipeline_directly5/step1_output.csv"
    duckdb_output_file = "memory://run_pipeline_directly5/step3_output.parquet"
    jinja2_output_file = "memory://run_pipeline_directly5/step5_output.json"

    sql_file        = os.path.join(working_dir, "Step2", "step2_with_duckdb.sql")
    j2_template_file = os.path.join(working_dir, "Step4", "step4.j2")

    ensure_dir_exists(http_output_file)
    ensure_dir_exists(duckdb_output_file)
    ensure_dir_exists(jinja2_output_file)

    step_executor = StepExecutor()

    try:
        logger.info("[Step 1: from_http]")
        http_step_config = {
            "name": "step1_from_http",
            "plugin": "from_http",
            "params": {
                "url": "http://localhost:8080/device_data.csv",
                "output_path": http_output_file
            }
        }
        http_result_container = step_executor.execute_step(http_step_config)
        print(http_result_container)
        if DataContainerStatus.ERROR == http_result_container.get_status():
            raise Exception("Step 1 failed, stopping pipeline execution.")

        logger.info("[Step 2: with_duckdb]")
        duckdb_step_config = {
            "name": "step2_with_duckdb",
            "plugin": "with_duckdb",
            "params": {
                "input_path": http_output_file,
                "output_path": duckdb_output_file,
                "query_file": sql_file,
                "table_name": "source_data"
            }
        }
        duckdb_result_container = step_executor.execute_step(
            duckdb_step_config,
            inputs={"input_data": http_result_container}
        )
        print(duckdb_result_container)
        if DataContainerStatus.ERROR == duckdb_result_container.get_status():
            raise Exception("Step 2 failed, stopping pipeline execution.")

        logger.info("[Step 3: with_jinja2]")
        jinja2_step_config = {
            "name": "step3_with_jinja2",
            "plugin": "with_jinja2",
            "params": {
                "input_path": duckdb_output_file,
                "output_path": jinja2_output_file,
                "template_path": j2_template_file
            }
        }
        jinja2_result_container = step_executor.execute_step(
            jinja2_step_config,
            inputs={"input_data": duckdb_result_container}
        )
        print(jinja2_result_container)
        if DataContainerStatus.ERROR == jinja2_result_container.get_status():
            raise Exception("Step 3 failed, stopping pipeline execution.")

        logger.info("--- Pipeline execution finished successfully! ---")

    except Exception:
        logger.error("\n--- An error occurred during pipeline execution ---")
        import traceback
        traceback.print_exc()

    finally:
        # パイプライン完了後にメモリを解放する
        from core.infrastructure.storage_adapter import storage_adapter
        storage_adapter.clear_memory(prefix="memory://run_pipeline_directly5/")
        logger.info("Memory store cleared.")


if __name__ == "__main__":
    main()