import requests

API_URL = "http://localhost:8000/api/v1/proxy/configured_service/configured_pipeline"

csv_data = """device_id,temperature,location_coords,measured_at
A001,23.5,"35.6895,139.6917","2025-09-19T19:00:00"
A002,24.1,"35.6896,139.6920","2025-09-19T19:05:00"
A003,22.8,"35.6897,139.6925","2025-09-19T19:10:00"
"""

response = requests.post(
    API_URL,
    data=csv_data.encode("utf-8"),
    headers={"Content-Type": "text/csv"}
)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())
