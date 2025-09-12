from setuptools import setup, find_packages

def parse_requirements(filename):
    with open(filename, 'r') as f:
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
    version="1.0.4",
    package_dir={"": "scripts"},
    packages=find_packages(where="scripts"),
    install_requires=parse_requirements('requirements.txt'),
)