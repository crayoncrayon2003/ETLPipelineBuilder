from ftplib import FTP, error_perm
import os

def ftp_client(host, port, username, password, filename):
    try:
        print(f"Connecting to FTP {host}:{port} ...")
        ftp = FTP()
        ftp.connect(host, port)
        ftp.login(username, password)
        print("Login successful.")

        # --- Get file list on the server ---
        print("\nFile list on server:")
        files = ftp.nlst()
        for f in files:
            print(f"  {f}")

        # --- Download the specified file ---
        if filename in files:
            print(f"\nDownloading file: {filename}")
            with open(filename, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            print(f"\nCSV data has been saved as '{filename}'.")
        else:
            print(f"\nThe specified file '{filename}' does not exist.")

        ftp.quit()

    except error_perm as e:
        print(f"FTP permission error: {e}")
    except ConnectionRefusedError:
        print("Connection error: The server is not running or the port is incorrect.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    SERVER_HOST = "127.0.0.1"
    SERVER_PORT = 2121

    ftp_client(SERVER_HOST, SERVER_PORT, "testuser", "local_secret_password_1234", "device_data.csv")
    ftp_client(SERVER_HOST, SERVER_PORT, "testuser", "local_secret_password_123" , "device_data.csv")
