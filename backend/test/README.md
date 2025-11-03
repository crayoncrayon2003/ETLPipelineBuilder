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

# 3. Run Tests
```
pydeps scripts/core --max-bacon=2 --show-deps --noshow > core_deps.txt
pydeps scripts/core --max-bacon=2 --show-deps --noshow --reverse > core_deps_reverse.txt
pydeps scripts/core --max-bacon=2 --show-deps -o core_deps.svg
```