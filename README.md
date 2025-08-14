# 1. BuckEnd App
## 1.1. Install
```
cd backend
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
```

## 1.2. Run : API Server
Start API Server
```
uvicorn api.main:app --reload
```

check swagger
```
http://127.0.0.1:8000/docs
```

check rest api
```
curl -X GET "http://127.0.0.1:8000/api/v1/plugins/"
```

## 1.3. uninstall
```
deactivate
rm -rf env
rm -rf prefect_data
```


# 2. FrontEnd App
```
cd frontend
nvm use
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
## 3.1 Start Dummy Server
```
pyton test/TestWebserver.py
```

## 3.2 FrontEnd App
The FrontEnd App is a pipeline editor.
Use FrontEnd App to edit, test, and save the pipeline.

### 3.2.1. Eddit Pipelines
Select a plugin from the menu on the left.
Selecting a plugin will display the plugin parameters on the right side.

#### Node1 : from_http
* Source URL        : http://localhost:8080/device_data.csv
* Output File Path  : ./test/data/Step1/device_data.csv

#### Node2 : with_duckdb
* Source File Path  : ./test/data/Step1/device_data.csv
* Output File Path  : ./test/data/Step3/device_data.csv
* SQL    File Path  : ./test/data/Step2/step2.sql

#### Node3 : with_jinja2
* Source File Path : ./test/data/Step3/device_data.csv
* Output File Path : ./test/data/Step5/device_data.csv
* j2     File Path : ./test/data/Step4/step4.j2

### 3.2.2. Run Test
Testing the pipeline with the "Run Test".

### 3.2.3. Save Pipelines
Save the pipeline with the "Save".
For example, save it in the pipeline.json file.
* Save File Path   : ./test/pipelines/pipeline.json

### 3.3. BuckEnd App
The BuckEnd App is a pipeline framework.
The BuckEnd App works without the FrontEnd App.

```
(env) python ./backend/run_pipeline.py ./test/pipelines/pipeline.json
```