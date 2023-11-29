-- Procedures and functions

-- COALESCE_API_RUN_VERSION
-- Returns the version of this set of procedures
-- eg CALL COALESCE_API_RUN_VERSION()

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_VERSION()
RETURNS NUMERIC(5,2)
LANGUAGE SQL
AS
$$
BEGIN
  RETURN 1.0;
END;
$$
;

-- COALESCE_API_RUN_JOB
-- Pass in JobID and EnvironmentID from Coalesce
-- Calls the startRun API to release a job 
-- Returns the runCounter of that job instance
-- eg CALL COALESCE_API_RUN_JOB(5, 3)

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_JOB(JobID NUMBER, EnvironmentID NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_job'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN, 'creds' = COALESCE_API_USER_PW)
as
$$
import _snowflake
import requests
import json

def run_job(session, JobID, EnvironmentID):
    token = _snowflake.get_generic_secret_string('token')
    sf_user = _snowflake.get_username_password('creds').username
    sf_pw = _snowflake.get_username_password('creds').password
    
    url = "https://app.coalescesoftware.io/scheduler/startRun"
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    payload = {
    "runDetails": {
        "parallelism": 16,
        "environmentID": str(EnvironmentID),
        "jobID": str(JobID)
        },
    "userCredentials": {
        "snowflakeAuthType": "Basic",
        "snowflakeUsername": sf_user,
        "snowflakePassword": sf_pw,
        "snowflakeWarehouse": "COMPUTE_WH",
        "snowflakeRole": "SYSADMIN"
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if "runCounter" in response.json():
            return response.json()['runCounter']
        else:
            return response.json()
    else:
        return "Error, response: " + str(response.status_code)

$$;


-- COALESCE_API_RESTART_JOB
-- Pass in runID from Coalesce
-- Calls the reRun API to release a job 
-- Returns the runCounter of that job instance
-- eg CALL COALESCE_API_RESTART_JOB(1488)

CREATE OR REPLACE PROCEDURE COALESCE_API_RESTART_JOB(RunID NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_job'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN, 'creds' = COALESCE_API_USER_PW)
as
$$
import _snowflake
import requests
import json

def run_job(session, RunID):
    token = _snowflake.get_generic_secret_string('token')
    sf_user = _snowflake.get_username_password('creds').username
    sf_pw = _snowflake.get_username_password('creds').password
    
    url = "https://app.coalescesoftware.io/scheduler/rerun"
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    payload = {
    "runDetails": {
        "parallelism": 16,
        "runID": str(RunID)
        },
    "userCredentials": {
        "snowflakeAuthType": "Basic",
        "snowflakeUsername": sf_user,
        "snowflakePassword": sf_pw,
        "snowflakeWarehouse": "COMPUTE_WH",
        "snowflakeRole": "SYSADMIN"
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if "runCounter" in response.json():
            return response.json()['runCounter']
        else:
            return response.json()
    else:
        return "Error, response: " + str(response.status_code)
$$;

-- FN_COALESCE_API_RUN_JOB
-- Pass in JobID and EnvironmentID from Coalesce
-- Calls the startRun API to release a job 
-- Returns the runCounter of that job instance
-- eg SELECT FN_COALESCE_API_RUN_JOB(5, 3)

CREATE OR REPLACE FUNCTION FN_COALESCE_API_RUN_JOB(JobID NUMBER, EnvironmentID NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_job'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('requests')
secrets = ('token' = COALESCE_API_TOKEN, 'creds' = COALESCE_API_USER_PW)
as
$$
import _snowflake
import requests
import json
session = requests.Session()

def run_job(JobID, EnvironmentID):
    token = _snowflake.get_generic_secret_string('token')
    sf_user = _snowflake.get_username_password('creds').username
    sf_pw = _snowflake.get_username_password('creds').password
    
    url = "https://app.coalescesoftware.io/scheduler/startRun"
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    payload = {
    "runDetails": {
        "parallelism": 16,
        "environmentID": str(EnvironmentID),
        "jobID": str(JobID)
        },
    "userCredentials": {
        "snowflakeAuthType": "Basic",
        "snowflakeUsername": sf_user,
        "snowflakePassword": sf_pw,
        "snowflakeWarehouse": "COMPUTE_WH",
        "snowflakeRole": "SYSADMIN"
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if "runCounter" in response.json():
            return response.json()['runCounter']
        else:
            return response.json()
    else:
        return "Error, response: " + str(response.status_code)
$$;


-- COALESCE_API_RUN_JOB_COMPLETE
-- Pass in JobID and EnvironmentID from Coalesce
-- Calls the startRun API to release a job and waits until that job is not running 
-- Returns the final run status of that job instance 
-- eg CALL COALESCE_API_RUN_JOB_COMPLETE(5, 3)

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_JOB_COMPLETE(JobID NUMBER, EnvironmentID NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_job'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN, 'creds' = COALESCE_API_USER_PW)
as
$$
import _snowflake
import requests
import json
import time


def run_job(session, JobID, EnvironmentID):
    token = _snowflake.get_generic_secret_string('token')
    sf_user = _snowflake.get_username_password('creds').username
    sf_pw = _snowflake.get_username_password('creds').password
    run_api     = "https://app.coalescesoftware.io/scheduler/startRun"
    status_api  = "https://app.coalescesoftware.io/scheduler/runStatus?runCounter="
    results_api = "https://app.coalescesoftware.io/api/v1/runs/!runID!/results"
    poll_secs = 30
    
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    payload = {
    "runDetails": {
        "parallelism": 16,
        "environmentID": str(EnvironmentID),
        "jobID": str(JobID)
        },
    "userCredentials": {
        "snowflakeAuthType": "Basic",
        "snowflakeUsername": sf_user,
        "snowflakePassword": sf_pw,
        "snowflakeWarehouse": "COMPUTE_WH",
        "snowflakeRole": "SYSADMIN"
        }
    }
    
    # Start the job
    run_response = requests.post(run_api, json=payload, headers=headers)
    b_loop = run_response.status_code == 200
    if b_loop:
        run_counter = str(run_response.json()["runCounter"])
        status_api = status_api + run_counter
        job_status = "Started"
        
        # If Job release was successful then loop until finished
        while b_loop:
            time.sleep(poll_secs)
            status_response = requests.get(status_api, headers=headers) 
            b_loop = status_response.status_code == 200
            # If Check Job Status was successful
            if b_loop:
                job_status = str(status_response.json()["runStatus"])
                b_loop = job_status == "running"

        return_msg = "Job results, run: " + run_counter + ". Status: " + job_status
        
    # If Job release was not successful
    else: 
        return_msg = "Run failed, Job Run API response: " + str(run_response.status_code) + ", Detail: " + str(run_response.json()["error"]["errorDetail"])
    return(return_msg)
$$;


-- FN_COALESCE_API_RUN_JOB_COMPLETE
-- Pass in JobID and EnvironmentID from Coalesce
-- Calls the startRun API to release a job and waits until that job is not running 
-- Returns the final run status of that job instance 
-- eg SELECT FN_COALESCE_API_RUN_JOB_COMPLETE(5, 3)

CREATE OR REPLACE FUNCTION FN_COALESCE_API_RUN_JOB_COMPLETE(JobID NUMBER, EnvironmentID NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_job'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN, 'creds' = COALESCE_API_USER_PW)
as
$$
import _snowflake
import requests
import json
import time
session = requests.Session()

def run_job(JobID, EnvironmentID):
    token = _snowflake.get_generic_secret_string('token')
    sf_user = _snowflake.get_username_password('creds').username
    sf_pw = _snowflake.get_username_password('creds').password
    run_api     = "https://app.coalescesoftware.io/scheduler/startRun"
    status_api  = "https://app.coalescesoftware.io/scheduler/runStatus?runCounter="
    results_api = "https://app.coalescesoftware.io/api/v1/runs/!runID!/results"
    poll_secs = 30
    
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    payload = {
    "runDetails": {
        "parallelism": 16,
        "environmentID": str(EnvironmentID),
        "jobID": str(JobID)
        },
    "userCredentials": {
        "snowflakeAuthType": "Basic",
        "snowflakeUsername": sf_user,
        "snowflakePassword": sf_pw,
        "snowflakeWarehouse": "COMPUTE_WH",
        "snowflakeRole": "SYSADMIN"
        }
    }
    
    # Start the job
    run_response = requests.post(run_api, json=payload, headers=headers)
    b_loop = run_response.status_code == 200
    if b_loop:
        run_counter = str(run_response.json()["runCounter"])
        status_api = status_api + run_counter
        job_status = "Started"
        
        # If Job release was successful then loop until finished
        while b_loop:
            time.sleep(poll_secs)
            status_response = requests.get(status_api, headers=headers) 
            b_loop = status_response.status_code  == 200
            # If Check Job Status was successful
            if b_loop:
                job_status = str(status_response.json()["runStatus"])
                b_loop = job_status == "running"

        return_msg = "Job results, run: " + run_counter + ". Status: " + job_status
        

    # If Job release was not successful
    else: 
        return_msg = "Run failed, Job Run API response: " + str(run_response.status_code) + ", Detail: " + str(run_response.json()["error"]["errorDetail"])  
    return(return_msg)
$$;



-- COALESCE_API_RUN_JOB_STATUS
-- Pass in RunID of the job instance
-- Calls the runStatus API to release a job and waits until that job is not running 
-- Returns the final run status of that job instance
-- eg CALL COALESCE_API_RUN_JOB_STATUS(1488)

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_JOB_STATUS(RunId NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_check'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN)
as
$$
import _snowflake
import requests
import json

def run_check(session, RunId):
    token = _snowflake.get_generic_secret_string('token')
    status_api  = "https://app.coalescesoftware.io/scheduler/runStatus?runCounter=" + str(RunId)
    poll_secs = 30
    
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    
    # Start the job
    status_response = requests.get(status_api, headers=headers)
    if status_response.status_code == 200:   
        if "runStatus" in status_response.json():
            return status_response.json()['runStatus']
        else:
            return status_response.json()
    else:
        msg = 'Error: ' + str(status_response.status_code)
    return(msg)
$$;


-- FN_COALESCE_API_RUN_JOB_STATUS
-- Pass in RunID of the job instance
-- Calls the runStatus API to release a job and waits until that job is not running 
-- Returns the final run status of that job instance
-- eg SELECT FN_COALESCE_API_RUN_JOB_STATUS(1488)

CREATE OR REPLACE FUNCTION FN_COALESCE_API_RUN_JOB_STATUS(RunId NUMBER)
RETURNS STRING
language python
runtime_version=3.8
handler = 'run_check'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests')
secrets = ('token' = COALESCE_API_TOKEN)
as
$$
import _snowflake
import requests
import json
session = requests.Session()

def run_check(RunId):
    token = _snowflake.get_generic_secret_string('token')
    status_api  = "https://app.coalescesoftware.io/scheduler/runStatus?runCounter=" + str(RunId)
    poll_secs = 30
    
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    
    # Start the job
    status_response = requests.get(status_api, headers=headers)
    if status_response.status_code == 200:   
        if "runStatus" in status_response.json():
            return status_response.json()['runStatus']
        else:
            return status_response.json()
    else:
        msg = 'Error: ' + str(status_response.status_code)
    return(msg)
$$;


-- COALESCE_API_RUNS_TABLE
-- Calls the api/v1/runs API  
-- Returns a table of job run information
-- eg SELECT * FROM TABLE(COALESCE_API_RUNS_TABLE())

CREATE OR REPLACE FUNCTION COALESCE_API_RUNS_TABLE()
RETURNS TABLE(
        ENVIRONMENT_ID NUMBER,
        RUN_TYPE STRING,
        RUN_ID NUMBER, 
        RUN_STATUS STRING,
        RUN_START_TIME STRING,
        RUN_END_TIME STRING)
language python
runtime_version=3.8
handler = 'api_run_details'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests','pandas')
secrets = ('token' = COALESCE_API_TOKEN)
as 
$$
import _snowflake
import requests
import json
from pandas import json_normalize 

class api_run_details:

    session = requests.Session()

    def process(self):
        token = _snowflake.get_generic_secret_string('token')
        url = "https://app.coalescesoftware.io/api/v1/runs"
        headers = {
            "accept": "application/json",
            "authorization": "Bearer " + token
        }
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        df_data = json_normalize(data['data'])
        for index, row in df_data.iterrows():
            yield(row['environmentID'],
                row['runType'],
                row['id'],
                row['runStatus'],
                row['runStartTime'],
                row['runEndTime'])

$$;





-- COALESCE_API_RUN_RESULTS
-- Pass in RunID of the job instance
-- Calls the api/v1/runs/results API  
-- Returns a table of a individual job run details
-- eg call COALESCE_API_RUN_RESULTS(1452)

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_RESULTS(RunID NUMBER)
RETURNS TABLE()
language python
runtime_version=3.8
handler = 'get_results'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests','pandas')
secrets = ('token' = COALESCE_API_TOKEN)
as
$$
import _snowflake
import requests
import json
from pandas import json_normalize 

def get_results(session, RunID):
    token = _snowflake.get_generic_secret_string('token')
    url = "https://app.coalescesoftware.io/api/v1/runs/" + str(RunID) + "/results"
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df_data = json_normalize(data['data'], record_path='queryResults', meta=['nodeID','name','isRunning'], record_prefix='query_')
    data_table = session.create_dataframe(df_data)
    return data_table
$$;


-- COALESCE_API_RUN_DETAILS_TABLE
-- Pass in RunID of the job instance
-- Calls the api/v1/runs/results API  
-- Returns a table of a individual job run details
-- eg SELECT * FROM TABLE(COALESCE_API_RUN_DETAILS_TABLE(1452))


CREATE OR REPLACE FUNCTION COALESCE_API_RUN_DETAILS_TABLE(RunID NUMBER)
RETURNS TABLE(
        NODE_ID STRING,
        NODE_NAME STRING, 
        IS_RUNNING BOOLEAN, 
        QUERY_NAME STRING,
        QUERY_START_TIME STRING,
        QUERY_END_TIME STRING,
        QUERY_IS_RUNNING BOOLEAN,
        QUERY_STATUS STRING, 
        QUERY_SUCCESS BOOLEAN, 
        QUERY_QUERY_ID STRING, 
        QUERY_SQL STRING,
        QUERY_ERROR_STRING STRING,
        QUERY_ERROR_DETAIL STRING)
language python
runtime_version=3.8
handler = 'api_run_details'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests','pandas')
secrets = ('token' = COALESCE_API_TOKEN)
as 
$$
import _snowflake
import requests
import json
from pandas import json_normalize 

class api_run_details:

    session = requests.Session()

    def process(self, RunID):
        token = _snowflake.get_generic_secret_string('token')
        url = "https://app.coalescesoftware.io/api/v1/runs/" + str(RunID) + "/results"
        headers = {
            "accept": "application/json",
            "authorization": "Bearer " + token
        }
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        df_data = json_normalize(data['data'], record_path='queryResults', meta=['nodeID','name','isRunning'], record_prefix='query_')
        df_data['query_error.errorString'] = df_data.get('query_error.errorString', 'N/A')  
        df_data['query_error.errorDetail'] = df_data.get('query_error.errorDetail', 'N/A')  
        for index, row in df_data.iterrows():
            yield(row['nodeID'],
                row['name'],
                row['isRunning'],
                row['query_name'],
                row['query_startTime'],
                row['query_endTime'],
                row['query_isRunning'],
                row['query_status'],
                row['query_success'],
                row['query_queryID'],
                row['query_sql'],
                row['query_error.errorString'],
                row['query_error.errorDetail'])

$$;


-- COALESCE_API_RUN_DETAIL_RESULTS
-- Pass in EnvironmentID, optional StartFrom for results filter in form datetime, optional ResultCount for number of rows returned
-- Calls the api/v1/runs API  
-- Returns a table of run results at detailed level
-- eg call COALESCE_API_RUN_DETAIL_RESULTS(3, '2023-09-29T19:20:29Z', 100 )
--    SELECT * FROM TABLE(RESULT_SCAN(-1));

CREATE OR REPLACE PROCEDURE COALESCE_API_RUN_DETAIL_RESULTS(EnvironmentID NUMBER, StartFrom STRING, Result_Count NUMBER)
RETURNS TABLE()
language python
runtime_version=3.8
handler = 'get_results'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests','pandas')
secrets = ('token' = COALESCE_API_TOKEN)
as
$$
import _snowflake
import requests
import json
from pandas import json_normalize 

def get_results(session, EnvironmentID, StartFrom, Result_Count):
    token = _snowflake.get_generic_secret_string('token')
    url_Start = ""
    url_Env = ""
    url_ResCount = ""
    if "T" in StartFrom and "-" in StartFrom and len(StartFrom) >= 15:
        url_Start = "&startingFrom=" + StartFrom.replace(":","%3A")
    if EnvironmentID > 0:
        url_Env = "&environmentID=" + str(EnvironmentID)
    if Result_Count > 0 and Result_Count <= 250:
        url_ResCount = "&limit=" + str(Result_Count)
    url = "https://app.coalescesoftware.io/api/v1/runs?detail=true&orderBy=id&orderByDirection=desc" + url_Start + url_Env + url_ResCount
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df_data = json_normalize(data['data'])
    data_table = session.create_dataframe(df_data)
    return data_table
$$;

-- RUN_JOB_SEND_EMAIL
-- Pass in JobID, EnvironmentID, Email_Recipient
-- Calls the startRun API to release a job and waits until that job is not running - then sends an email
-- Returns complete message 
-- eg CALL RUN_JOB_SEND_EMAIL(5, 3, 'doug.barrett@coalesce.io,someone@coalesce.io');

CREATE OR REPLACE PROCEDURE RUN_JOB_SEND_EMAIL(JobID NUMBER, EnvironmentID NUMBER, Email_Recipients STRING)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
DECLARE
    email_title STRING;
    job_return STRING;
BEGIN
    email_title := 'EnvironmentID: ' || TO_CHAR(:EnvironmentID) || ', JobID: ' || TO_CHAR(:JobID);
    job_return := FN_COALESCE_API_RUN_JOB_COMPLETE(:JobID, :EnvironmentID); 
    IF (RIGHT(:job_return,9) = 'completed')  THEN
        email_title := 'Coalesce Job Completed. ' || :email_title ;
        CALL SYSTEM$SEND_EMAIL('COALESCE_EMAIL', :Email_Recipients, :email_title, :job_return);
    ELSE
        email_title := 'Coalesce Job failed. ' || :email_title ;
        CALL SYSTEM$SEND_EMAIL('COALESCE_EMAIL', :Email_Recipients, :email_title, :job_return);
    END IF;
    job_return := :job_return || '. Email sent.';
    RETURN job_return ;
END;
$$;

-- COALESCE_API_NODES
-- Pass in EnvironmentID
-- Calls the api/v1/environments/nodes 
-- Returns a table of node information
-- eg call COALESCE_API_NODES(3 )
--    SELECT * FROM TABLE(RESULT_SCAN(-1));

CREATE OR REPLACE PROCEDURE COALESCE_API_NODES(EnvironmentID NUMBER)
RETURNS TABLE()
language python
runtime_version=3.8
handler = 'get_results'
external_access_integrations=(COALESCE_API_INTEGRATION)
packages = ('snowflake-snowpark-python','requests','pandas')
secrets = ('token' = COALESCE_API_TOKEN)
as
$$
import _snowflake
import requests
import json
from pandas import json_normalize 

def get_results(session, EnvironmentID):
    token = _snowflake.get_generic_secret_string('token')
    url = "https://app.coalescesoftware.io/api/v1/environments/"+str(EnvironmentID)+"/nodes"
    headers = {
        "accept": "application/json",
        "authorization": "Bearer " + token
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df_data = json_normalize(data['data'])
    data_table = session.create_dataframe(df_data)
    return data_table
$$;
