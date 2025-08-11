from setuptools import setup, find_packages

setup(
    name="etl_framework_backend",
    version="1.0.0",
    package_dir={"": "scripts"},
    packages=find_packages(where="scripts"),
)