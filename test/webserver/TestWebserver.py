
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import random
import json
from datetime import datetime, timedelta, timezone

# --- Server Configuration ---
HOST = "0.0.0.0"  # Listen on all available network interfaces
PORT = 8080       # Port to listen on
CSV_FILENAME = "device_data.csv" # The filename clients will request


def _generate_dynamic_csv() -> str:
    """
    Generates CSV data with a random number of rows and dynamic timestamps.

    Returns:
        str: A string containing the full CSV data.
    """
    # 1. Initialize with the header row.
    csv_lines = ["device_id,temperature,location_coords,measured_at"]

    # 2. Determine a random number of data rows.
    num_rows = random.randint(1, 10)

    # 3. Get the current time in UTC.
    current_time = datetime.now(timezone.utc)

    locations = [
        "35.681236,139.767125", # Tokyo
        "40.712776,-74.005974", # New York
        "48.856613,2.352222",   # Paris
        "51.507351,-0.127758",  # London
    ]

    # 4. Generate random data rows.
    for i in range(num_rows):
        device_id = f"DEV-{i+1:03d}"
        temperature = round(random.uniform(15.0, 30.0), 2)
        location = random.choice(locations)
        # Subtract N seconds from the current time for each row
        measured_time = current_time - timedelta(seconds=i * 10)
        timestamp = measured_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        csv_lines.append(f'{device_id},{temperature},"{location}",{timestamp}')

    # Join all lines into a single string and return.
    return "\n".join(csv_lines)

def _generate_static_csv() -> str:
    csv_lines = ["device_id,temperature,location_coords,measured_at"]
    csv_lines.append('DEV-001,19.95,"48.856613,2.352222",2025-08-16T02:12:03Z')
    csv_lines.append('DEV-002,20.35,"51.507351,-0.127758",2025-08-16T02:11:53Z')
    csv_lines.append('DEV-003,22.69,"48.856613,2.352222",2025-08-16T02:11:43Z')
    csv_lines.append('DEV-004,28.44,"40.712776,-74.005974",2025-08-16T02:11:33Z')

    return "\n".join(csv_lines)

class HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Check if the requested path matches the target CSV filename
        if self.path == f"/{CSV_FILENAME}":

            # Dynamically generate the CSV data in memory
            # csv_data = _generate_dynamic_csv()
            csv_data = _generate_static_csv()

            # Encode the string data to a UTF-8 byte sequence
            response_body = csv_data.encode('utf-8')

            # Send a success (200 OK) response
            self.send_response(200)

            # Send the response headers
            self.send_header('Content-type', 'text/csv; charset=utf-8')
            self.send_header('Content-Length', str(len(response_body)))
            self.send_header('Content-Disposition', f'attachment; filename="{CSV_FILENAME}"')
            self.end_headers()

            # Send the response body (the actual CSV data)
            self.wfile.write(response_body)
        else:
            # If the path is incorrect, send a 404 Not Found error
            self.send_error(404, f"Not Found: Please access /{CSV_FILENAME}")

    def do_POST(self):
        content_type = self.headers.get('Content-Type')
        if content_type != 'application/json':
            self.send_error(400, "Content-Type must be application/json")
            return

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        print("=== Received JSON ===")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("=====================")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        response = {"status": "success", "message": "JSON received and printed"}
        self.wfile.write(json.dumps(response).encode("utf-8"))

if __name__ == "__main__":
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, HTTPHandler)

    print(f"Starting test server on http://{HOST}:{PORT}")
    print(f"Serving CSV at: http://localhost:{PORT}/{CSV_FILENAME}")
    print(f"Accepting JSON via POST at: http://localhost:{PORT}/")
    print("Press Ctrl+C to stop the server.")

    httpd.serve_forever()