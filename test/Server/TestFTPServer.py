import os
from pyftpdlib.servers import FTPServer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.authorizers import DummyAuthorizer

# --- Server Configuration ---
HOST = "0.0.0.0"
PORT = 2121

EXPECTED_USERNAME = "testuser"
EXPECTED_PASSWORD = "local_secret_password_123"

current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
FTP_ROOT = os.path.join(current_dir, "ftp_root")

def run_ftp_server():
    authorizer = DummyAuthorizer()
    authorizer.add_user(EXPECTED_USERNAME, EXPECTED_PASSWORD, str(FTP_ROOT), perm="elradfmw")
    authorizer.add_anonymous(str(FTP_ROOT))

    handler = FTPHandler
    handler.authorizer = authorizer

    server = FTPServer((HOST, PORT), handler)
    print(f"FTP server started at {FTP_ROOT}")
    server.serve_forever()

if __name__ == "__main__":
    run_ftp_server()
