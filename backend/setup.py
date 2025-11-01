import os
from setuptools import setup, find_packages

BASE_DIR = os.path.dirname(__file__)

def parse_requirements(filename):
    req_path = os.path.join(BASE_DIR, filename)
    with open(req_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    reqs = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        reqs.append(line)
    return reqs

setup(
    name="etl_framework_backend",
    version="1.1.1",
    package_dir={"": "scripts"},
    packages=find_packages(where="scripts"),
    install_requires=parse_requirements("requirements.txt"),
)
