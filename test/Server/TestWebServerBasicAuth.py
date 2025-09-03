# test_http_server.py

from http.server import HTTPServer, BaseHTTPRequestHandler
import base64
import json
import random
from datetime import datetime, timedelta, timezone

# --- Server Configuration ---
HOST = "0.0.0.0"
PORT = 8080
CSV_FILENAME = "device_data.csv"

EXPECTED_USERNAME = "testuser"
EXPECTED_PASSWORD = "local_secret_password_123"


def _generate_static_csv() -> str:
    """Generates a consistent CSV string for testing."""
    csv_lines = [
        "device_id,temperature,location_coords,measured_at",
        'DEV-001,19.95,"48.856613,2.352222",2025-08-16T02:12:03Z',
        'DEV-002,20.35,"51.507351,-0.127758",2025-08-16T02:11:53Z',
        'DEV-003,22.69,"48.856613,2.352222",2025-08-16T02:11:43Z',
        'DEV-004,28.44,"40.712776,-74.005974",2025-08-16T02:11:33Z',
    ]
    return "\n".join(csv_lines)

class HTTPHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler with Basic Authentication for all endpoints.
    """
    def _handle_auth(self) -> bool:
        """
        Checks for valid Basic Authentication headers.
        Returns True if authentication is successful, False otherwise.
        """
        auth_header = self.headers.get('Authorization')

        # 1. No Authorization header provided
        if auth_header is None:
            self.send_response(401) # Unauthorized
            self.send_header('WWW-Authenticate', 'Basic realm="Test Server - Credentials Required"')
            self.end_headers()
            self.wfile.write(b"Authorization required.")
            return False

        # 2. Header is not of type 'Basic'
        if not auth_header.startswith('Basic '):
            self.send_error(400, "Invalid Authorization header: must be 'Basic'.")
            return False

        try:
            # 3. Decode and check credentials
            encoded_credentials = auth_header.split(' ')[1]
            decoded_bytes = base64.b64decode(encoded_credentials)
            credentials = decoded_bytes.decode('utf-8')
            username, password = credentials.split(':', 1)

            if username == EXPECTED_USERNAME and password == EXPECTED_PASSWORD:
                # Credentials are correct
                return True
            else:
                # Credentials are incorrect
                self.send_error(403, "Forbidden: Invalid username or password.")
                return False
        except Exception:
            self.send_error(400, "Invalid Authorization header format.")
            return False

    def do_GET(self):
        """Handles GET requests after checking authentication."""
        if not self._handle_auth():
            # If authentication fails, do not proceed.
            return

        # --- If authenticated, proceed with the original logic ---
        if self.path == f"/{CSV_FILENAME}":
            csv_data = _generate_static_csv()
            response_body = csv_data.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-type', 'text/csv; charset=utf-8')
            self.send_header('Content-Length', str(len(response_body)))
            self.send_header('Content-Disposition', f'attachment; filename="{CSV_FILENAME}"')
            self.end_headers()
            self.wfile.write(response_body)
        else:
            self.send_error(404, f"Not Found: Please access /{CSV_FILENAME}")

    def do_POST(self):
        """Handles POST requests after checking authentication."""
        if not self._handle_auth():
            # If authentication fails, do not proceed.
            return

        # --- If authenticated, proceed with the original logic ---
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

        print("\n=== Received JSON via POST ===")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("==========================")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        response = {"status": "success", "message": "JSON received"}
        self.wfile.write(json.dumps(response).encode("utf-8"))


if __name__ == "__main__":
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, HTTPHandler)

    print(f"Starting test HTTP server on http://{HOST}:{PORT}")
    print(f"Serving CSV with Basic Auth at: http://localhost:{PORT}/{CSV_FILENAME}")
    print(f"Accepting JSON with Basic Auth via POST at: http://localhost:{PORT}/")
    print("\n--- Credentials for Testing ---")
    print(f"Username: {EXPECTED_USERNAME}")
    print(f"Password: {EXPECTED_PASSWORD}")
    print("-----------------------------\n")
    print("Press Ctrl+C to stop the server.")

    httpd.serve_forever()