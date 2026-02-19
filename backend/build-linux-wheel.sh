set -e

if [ ! -d "env" ]; then
    python3.9 -m venv env
fi

source env/bin/activate

pip install --upgrade pip setuptools wheel
python install_requirements.py

pip install -e .

rm -rf dist/
rm -rf build/
pip install build
python -m build