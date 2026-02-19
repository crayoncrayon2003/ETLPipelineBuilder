$ErrorActionPreference = "Stop"

if (-not (Test-Path "env")) {
    python3.9 -m venv env
}

& env\Scripts\Activate.ps1

pip install --upgrade pip setuptools wheel
python install_requirements.py

pip install -e .

if (Test-Path "dist") { Remove-Item -Recurse -Force dist }
if (Test-Path "build") { Remove-Item -Recurse -Force build }
pip install build
python -m build