import os
import subprocess
import sys

def install_packages(packages):
    """Install the specified list of packages using pip."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", *packages])

def main():
    # Determine if running in AWS Glue environment
    is_glue = "AWS_EXECUTION_ENV" in os.environ

    if is_glue:
        print("AWS Glue environment detected. Skipping additional installation.")
    else:
        print("Local environment detected. Installing requirements.txt")
        # First, install packages from requirements.txt
        install_packages(["-r", "requirements.txt"])
        print("Local environment detected. Installing requirements-test.txt")
        install_packages(["-r", "requirements-test.txt"])

        # Then, install PySpark and Delta Lake
        extra_packages = [
            "pyspark==3.5.4",
            "delta-spark==3.3.0"
        ]
        print(f"Installing additional packages: {extra_packages}")
        install_packages(extra_packages)

    print("Installation complete.")

if __name__ == "__main__":
    main()
