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
python ./scripts/api/main.py
```
or
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
Please refer to the from_http_with_basic_auth plugin.
To access a web server with basic authentication, you must configure the username and password within its parameters.

For example, a direct configuration might look like this (though this is not recommended for sensitive information):
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path": "./test/data/Step1/device_data.csv",
  "username": "testuser",
  "password": "local_secret_password_123"
}
```

When running your Backend Apps, you'll typically encounter two scenarios for managing credentials:


### Scenario 1: Local Development & Testing via GUI
You want to verify that the from_http_with_basic_auth configuration with authentication credentials can access the web server during local development or when testing the pipeline via the GUI.


### Scenario 2: Deployment on AWS
You want to securely set credentials stored in AWS Secret Management services (Secrets Manager, Parameter Store, KMS) when running on AWS environments (e.g., AWS Lambda, AWS Glue).


The Backend App provides a robust mechanism to handle these scenarios using backend/scripts/core/infrastructure/secret_resolver.py. This resolver automatically determines the execution environment (local or AWS) and fetches secrets from the appropriate source based on explicit prefixes in your configuration.

### How to Configure Credentials:
Define your authentication credentials. For example:
```
MY_HTTP_BASIC_USERNAME="testuser"
MY_HTTP_BASIC_PASSWORD="local_secret_password_123"
```

### For Scenario 1 (Local Development):
Then, configure your plugin parameters using the env:// prefix for local environment variables:
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path”: “./test/data/Step1/device_data.csv",
  "username”: "${secrets.env://MY_HTTP_BASIC_USERNAME}",
  "password”: "${secrets.env://MY_HTTP_BASIC_PASSWORD}"
}
```

### For Scenario 2 (Deployment on AWS):
Do not use backend/.env. Instead, leverage AWS Secret Management services.
The secret_resolver supports fetching secrets from AWS Secrets Manager, AWS Systems Manager Parameter Store, and decrypting data with AWS Key Management Service (KMS) using explicit prefixes.

Here are examples of how to configure your plugin parameters for AWS environments:

#### Using AWS Secrets Manager:
```
"params": {
  "url": "http://localhost:8080/device_data.csv",
  "output_path": "./test/data/Step1/device_data.csv",
  "username": "${secrets.aws_secretsmanager://prod/MyApi/credentials@username}",
  "password": "${secrets.aws_secretsmanager://prod/MyApi/credentials@password}"
}
```

#### Using AWS Systems Manager Parameter Store:
```
"params": {
  "url": "https://service.example.com/info",
  "output_path": "./output/info.txt",
  "username": "${secrets.aws_parameterstore:///prod/MyService/HttpUser}",
  "password": "${secrets.aws_parameterstore:///prod/MyService/HttpPassword?with_decryption=true}"
}
```

#### Using AWS Key Management Service (KMS) for decryption:
```
"params": {
  "url": "https://secure-api.example.com/data",
  "output_path": "./output/secure_data.json",
  "api_token": "${secrets.aws_kms_decrypt://AQICAHg...[Base64 Encoded Ciphertext]...}"
}
```

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
* Python library path            : s3://<bucket_name>/lib/<wheelname>.whl

Sample code for Glue Python Shell

```
from core.pipeline.step_executor import StepExecutor
from core.data_container.container import DataContainer

step_executor = StepExecutor()

http_params = {
    "url": "https://<sample.com>/device_data.csv",
    "output_path": "s3://<bucket_name>/device_data2.csv",
    "username": "${secrets.aws_secretsmanager://<secretsmanager_name>@<key_username>}",
    "password": "${secrets.aws_secretsmanager://<secretsmanager_name>@<key_password>}"
}
http_step_config = {
    "name": "step1",
    "plugin": "from_http_with_basic_auth",
    "params": http_params
}
http_result_container = step_executor.execute_step(http_step_config, inputs={})


duckdb_params = {
    "input_path": "s3://<bucket_name>/device_data.csv",
    "input_encoding": "cp932",
    "output_path": "s3://<bucket_name>/run_pipeline_directly.parquet",
    "query_file": "s3://<bucket_name>/step2.sql",
    "table_name": "source_data"
}
duckdb_step_config = {
    "name": "step2_with_duckdb",
    "plugin": "with_duckdb",
    "params": duckdb_params
}
duckdb_result_container = step_executor.execute_step(duckdb_step_config, inputs={"input_data": http_result_container})
```