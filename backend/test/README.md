# 1. Setup
```
cd backend
python3.9 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) python install_requirements.py
(env) pip install -e .
```

# 2. Run Tests
```
cd backend/test
python run_all_tests.py
```
