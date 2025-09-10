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
python3.9 -m venv env
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
python3.9 -m venv env
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

## 3.3. How to Use : Case1 Basic
### 3.3.1. Run Dummy Server
```
pyton test/Server/TestWebServer.py
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
* Output File Path  : ./test/data/Step3/run_pipeline_gui.csv
* SQL    File Path  : ./test/data/Step2/step2.sql

#### Node3 : with_jinja2
* Source File Path : ./test/data/Step3/run_pipeline_gui.csv
* Output File Path : ./test/data/Step5/run_pipeline_gui.json
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

## 3.3. How to Use : Case2 Authentication
Please refer to the `from_http_with_basic_auth` plugin.
To access a web server with basic authentication, you must configure the username and password in `from_http_with_basic_auth`.
For example:
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path": "./test/data/Step1/device_data.csv",
  "username": "testuser",
  "password": "local_secret_password_123"
}
```

When running Backend Apps on AWS, you'll face the following issue:
#1. When testing the pipeline via GUI : You want to verify that the `from_http_with_basic_auth` configuration with authentication credentials can access the web server.
#2. When running on AWS : You want to set the credentials stored in the secret manager to `from_http_with_basic_auth`.

The backend App has a mechanism to solve this issue.
refer to backend/scripts/core/secrets/secret_resolver.py

When executing #1, define your authentication credentials in backend/.env.
For example:
　MY_HTTP_BASIC_USERNAME="testuser"
　MY_HTTP_BASIC_PASSWORD="local_secret_password_123"

The configuration should be as follows:
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path”: “./test/data/Step1/device_data.csv",
  "username”: ${secrets.MY_HTTP_BASIC_USERNAME},
  "password”: ${secrets.MY_HTTP_BASIC_PASSWORD}
}
```

When executing #2, do not use backend/.env.
The configuration should be as follows:
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path": "./test/data/Step1/device_data.csv",
  "username": ${secrets.prod/MyApi/credentials@username},
  "password": ${secrets.prod/MyApi/credentials@password}
}
```

secrets.prod/MyApi/credentials is the secret name in AWS Secrets Manager.
username/password is the key in the JSON stored in the secret.


## 3.3. How to Use : Case3 AWS
Run the following command locally
```
git clone https://github.com/crayoncrayon2003/ETLPipelineBuilder.git
cd ETLPipelineBuilder/backend
python3.9 -m venv env
source env/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
python setup.py sdist bdist_wheel

aws s3 cp dist/<name>.whl s3://<bucket_name>/lib/
```

Configure the following in AWS. ex.Glue
* Script path    (default)       : s3://<bucket_name>/scripts/
* Temporary path (default)       : s3://<bucket_name>/temporary/
* Python library path            : s3://<bucket_name>/lib/<name>.whl

Sample code for Glue Python Shell

```
from core.plugin_manager.manager import framework_manager
from core.data_container.container import DataContainer

http_params = {
    "url": "https://<sample.com>/device_data.csv",
    "output_path": "s3://<bucket_name>/device_data.csv"
}
http_result_container = framework_manager.call_plugin_execute(
    plugin_name="from_http",
    params=http_params,
    inputs={}
)

duckdb_params = {
    "input_path": "s3://<bucket_name>/device_data.csv",
    "input_encoding": "cp932",
    "output_path": "s3://<bucket_name>/run_pipeline_directly.parquet",
    "query_file": "s3://<bucket_name>/step2.sql",
    "table_name": "source_data"
}
duckdb_result_container = framework_manager.call_plugin_execute(
    plugin_name="with_duckdb",
    params=duckdb_params,
    inputs={"input_data": http_result_container}
)
```
