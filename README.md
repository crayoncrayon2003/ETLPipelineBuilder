# 1. Summary
This application is a tool for implementing ETL pipelines.
There are two components:

* Backend App  : The core components of this tool.
* FrontEnd App : The support components of Backend App.

Backend App is a framework for implementing ETL pipelines.

ETL pipeline functions can be added in the form of plugins.

Plugins implement the following:
* Their own names
* Their own parameter schemas
* Their own functions

The Backend App provides a framework for calling plugin groups.
The backend application runs independently as an ETL pipeline.
Refer to "./backend/run_pipeline_directly.py".

This sample hard-codes the plugin call.　Coding is tedious.
The FrontEnd App supports backend pipeline coding with a GUI.
In order to connect with the FrontEnd App, the Backend App has a REST API.


# 2. Only BuckEnd App
## 2.1. Setup
```
cd backend
python3.12 -m venv env
source env/bin/activate
(env) pip install --upgrade pip setuptools wheel
(env) pip install -r requirements.txt
(env) pip install -e .
```

## 2.2. Run
```
(env) python run_pipeline_directly.py
```

# 3. BuckEnd App with FrontEnd App
## 3.1. BuckEnd App
### 3.1.1. Setup
```
cd backend
python3.12 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
```

### 3.1.2. Run API Server for FrontEnd App
Start API Server of BuckEnd App
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

## 3.2. FrontEnd App
### 3.2.1. Setup
```
cd frontend
nvm use
node -v
> v20.x.x

npm install
npm install @rjsf/core @rjsf/validator-ajv8 @rjsf/mui @mui/material@^6.5.0 @mui/icons-material@^6.5.0 @emotion/react @emotion/styled electron electron-builder electron-is-dev concurrently
```

### 3.2.2. Run FrontEnd App
```
npm start
```
or
```
npm run dev
```

## 3.3. How to Use
### 3.3.1. Run Dummy Server
```
pyton test/TestWebserver.py
```

### 3.3.2. FrontEnd App
#### 3.3.2.1. Run Test
The FrontEnd App is a pipeline editor.
Use FrontEnd App to edit, test, and save the pipeline.

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

#### 3.3.2.2. Run Test
Testing the pipeline with the "Run Test".

#### 3.3.2.3. Save Pipelines
Save the pipeline with the "Save".
For example, save it in the pipeline.json file.
* Save File Path   : ./test/pipelines/pipeline.json

### 3.3.3. BuckEnd App
The BuckEnd App is a pipeline framework.
The BuckEnd App works without the FrontEnd App.

```
(env) python run_pipeline_with_parameter_file.py ../test/pipelines/pipeline.json
```

### 3.3.4. clean up
### 3.3.4.1. BuckEnd App
```
deactivate
rm -rf env
```

### 3.3.4.2. FrontEnd App
```
rm -rf node_modules
rm package-lock.json
```