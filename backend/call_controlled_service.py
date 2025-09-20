import os
import requests
import json

API_URL = "http://127.0.0.1:8000/api/v1/proxy/controlled_service"

csv_data = """device_id,temperature,location_coords,measured_at
A001,23.5,"35.6895,139.6917","2025-09-19T19:00:00"
A002,24.1,"35.6896,139.6920","2025-09-19T19:05:00"
A003,22.8,"35.6897,139.6925","2025-09-19T19:10:00"
"""

script_dir = os.path.dirname(os.path.abspath(__file__))
working_dir = os.path.abspath(os.path.join(script_dir, "..", "test", "data"))

duckdb_output_file = os.path.join(working_dir, "Step3", "run_pipeline_directly.parquet")
jinja2_output_file = os.path.join(working_dir, "Step5", "run_pipeline_directly.json")
sql_file = os.path.join(working_dir, "Step2", "step2_with_duckdb.sql")
j2_template_file = os.path.join(working_dir, "Step4", "step4.j2")

steps = [
    {
        "plugin": "with_duckdb",
        "params": {
            "query_file": sql_file,
            "output_path": duckdb_output_file,
            "table_name": "source_data"
        }
    },
    {
        "plugin": "with_jinja2",
        "params": {
            "input_path": duckdb_output_file,
            "output_path": jinja2_output_file,
            "template_path": j2_template_file
        }
    },
    {
        "plugin": "to_http",
        "params": {
            "input_path": jinja2_output_file,
            "url": "http://localhost:8080/"
        }
    }
]

params = {
    "steps_json": json.dumps(steps),
    "storage_dir": "/tmp/proxy_output"
}

response = requests.post(
    API_URL,
    params=params,
    data=csv_data.encode("utf-8"),
    headers={"Content-Type": "text/csv"}
)

print("Status Code:", response.status_code)
try:
    result = response.json()
    print("Response JSON:")
    print(json.dumps(result, indent=2))

    if "final_metadata" in result:
        print("\n--- result ---")
        for k, v in result["final_metadata"].items():
            print(f"{k}: {v}")
except Exception:
    print("Raw Response:")
    print(response.text)
