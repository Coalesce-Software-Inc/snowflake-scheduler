# snowflake-scheduler

## Introduction 

This repo describes an approach and contains the code necessary to use Coalesce’s web APIs and Snowflakes API Integration to simplify scheduling jobs in Coalesce.  More documentation on Coalesce’s APIs can be found here:  https://docs.coalesce.io/reference/startrun 

Included in this documentation are instructions for installing these stored procedures and functions in your Snowflake environment.  And a discussion on how it could be used.  

The Procedures and Functions included are:

| Name | Type | Input | Output | Coalesce API |
|------|------|-------|--------|--------------|
| COALESCE_API_RUN_JOB | Stored proc | jobId, envId | runCounter | startRun |
| COALESCE_API_RUN_JOB_STATUS | Stored proc | runCounter | jobStatus | runStatus |
| COALESCE_API_RESTART_JOB | Stored proc | runCounter | runCounter | rerun |
| FN_COALESCE_API_RUN_JOB_STATUS | Function | runCounter | jobStatus | runStatus |
| COALESCE_API_RUN_JOB_COMPLETE | Stored proc | jobId, envId | runCounter, jobStatus | startRun, runStatus |
| FN_COALESCE_API_RUN_JOB_COMPLETE | Function | jobId, envId | runCounter, jobStatus | startRun, runStatus |
| COALESCE_API_RUNS_TABLE | Table Function || Table of Run List | runs |
| COALESCE_API_RUN_DETAILS_TABLE | Table Function | runCounter | Table of Run Details | runs/results
| RUN_JOB_SEND_EMAIL | Stored Proc | jobid, envid, emails | runCounter, jobStatus | startRun, runStatus |

**Note**: the installation requires an External Integration which is a preview feature in Snowflake:
PREVIEW FEATURE— OPEN

Preview support for this feature is available to accounts on AWS except the Gov region. It’s also available in limited preview to accounts on Azure; contact your account representative for access.


## Setup Instructions
These steps only need to be run once to create them in Snowflake.  The pre-requisite DDL needs elevated permissions (SYSADMIN).  

The Setup SQLs should be run in a separate database or schema that you create so they are not lost with your Data Warehouse nodes e.g. COALESCE.UTILITY

**Note**: The Warehouse and Role used to execute jobs is hard coded in the Procedure / Functions and can be modified depending on your requirements. Search and replace the ```COMPUTE_WH``` and ```SYSADMIN``` text in the setup script below.   

### Step 1
Open the ```prerequisites.sql``` file to create the necessary dependencies for the Snowflake scheduler. The items created are listed below:

- Network rule for Snowflake to reach Coalesce
- A Secret containing your token from Coalesce. You can generate a token from the deploy interface of Coalesce.
- A Secret for the service account that will run your jobs
- An integration to access the Coalesce API.
- **Optional** Email integration to send emails about job status


### Step 2
Open the ```procs_and_functions.sql``` file and run the code contained within. This code will generate the stored procedures and functions necessary to run Coalesce jobs directly in Snowflake. 

Each code block is documented to provide context into the actions being performed. 


### Step 3

Open the ```testing_examples.sql``` and run any of the examples provided to ensure the stored procedures and functions have been properly created. You will need to replace any values for ```runCounter```, ```jobID```, and ```EnvironmentID``` with the values from your Coalesce account and Snowflake runs. 

## Scheduling 

You can use tasks in Snowflake to schedule your Coalesce jobs. By opening the ```schedule_examples.sql``` file, you can view a few examples that can be modified to meet the scheduling needs for your Coalesce jobs. 

## Support

Should you encounter any issues while going through the Snowflake scheduler process, please reach out to us by visiting [help.coalesce.io](https://help.coalesce.io/hc/en-us/requests/new?ticket_form_id=21204431758995)












