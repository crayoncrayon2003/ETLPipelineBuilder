from pyftpdlib.servers import FTPServer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.authorizers import DummyAuthorizer
from pathlib import Path

# --- Server Configuration ---
HOST = "0.0.0.0"
PORT = 2121

EXPECTED_USERNAME = "testuser"
EXPECTED_PASSWORD = "local_secret_password_123"

FTP_ROOT = Path("./ftp_root").resolve()

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
