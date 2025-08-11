# 1. buckend
# 1.1. install
```
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
```

# 1.2. run
```
uvicorn backend.api.main:app --reload
```

check swagger
```
http://127.0.0.1:8000/docs
```

check rest api
```
curl -X GET "http://127.0.0.1:8000/api/v1/plugins/"
```

# 1.3. uninstall
```
deactivate
rm -rf env
```


# 2. Front
```
node -v
> v20.x.x
```

## 2.1 install
```
npm install
npm install @rjsf/core @rjsf/validator-ajv8 @rjsf/mui @mui/material@^6.5.0 @mui/icons-material@^6.5.0 @emotion/react @emotion/styled electron electron-builder electron-is-dev concurrently
```

## 2.2 run
```
npm start
```
or
```
npm run dev
```


# 2.3. uninstall
```
rm -rf node_modules
rm package-lock.json
```

# 3. How to Use
## 3.1 Dummy Server
```
pyton test/TestWebserver.py
```

## 3.2 ETL Tool
### Node1 : from_http
* Source URL        : http://localhost:8080/device_data.csv
* Output File Path  : ../test/data/Step1/device_data.csv

### Node2 : with_duckdb
* Source File Path  : ../test/data/Step1/device_data.csv
* Output File Path  : ../test/data/Step3/device_data.csv
* SQL    File Path  : ../test/data/Step2/step2.sql

### Node3 : ■with_jinja2
* Source File Path : ../test/data/Step3/device_data.csv
* Output File Path : ../test/data/Step5/device_data.csv
* j2     File Path : ../test/data/Step4/step4.j2

