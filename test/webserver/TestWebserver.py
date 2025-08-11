# test_server.py

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

# --- Server Configuration ---
HOST = "0.0.0.0"  # Listen on all available network interfaces
PORT = 8080       # Port to listen on
CSV_FILENAME = "device_data.csv" # The filename clients will request

class HTTPHandler(BaseHTTPRequestHandler):
    """
    A simple HTTP request handler that serves a dynamically generated CSV file.
    """
    def do_GET(self):
        """Handles GET requests."""

        # Check if the requested path matches the target CSV filename
        if self.path == f"/{CSV_FILENAME}":

            # --- Start of Modification: Update CSV data ---
            # Dynamically generate the CSV data in memory
            csv_data = (
                "device_id,temperature,location_coords,measured_at\n"
                "DEV-001,25.5,\"35.681236,139.767125\",2025-08-11T12:00:00Z\n"
                "DEV-002,26.1,\"40.712776,-74.005974\",2025-08-11T12:01:00Z\n"
                "DEV-003,24.9,\"48.856613,2.352222\",2025-08-11T12:02:00Z\n"
            )
            # --- End of Modification ---

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


if __name__ == "__main__":
    # Create and start the HTTP server
    server_address = (HOST, PORT)
    httpd = HTTPServer(server_address, HTTPHandler)

    print(f"Starting test server on http://{HOST}:{PORT}")
    print(f"Serving CSV at: http://localhost:{PORT}/{CSV_FILENAME}")
    print("Press Ctrl+C to stop the server.")

    httpd.serve_forever()