-- Run interactively

-- Check which version of the Procs
-- CALL COALESCE_API_RUN_VERSION()  

-- Run a job and return
-- CALL COALESCE_API_RUN_JOB(JobID NUMBER, EnvironmentID NUMBER)  
CALL COALESCE_API_RUN_JOB(5, 3); 

-- Check job status using procedure
-- CALL COALESCE_API_RUN_JOB_STATUS(runCounter NUMBER) 
CALL COALESCE_API_RUN_JOB_STATUS(1457); 

-- Check job status using function
-- SELECT FN_COALESCE_API_RUN_JOB_STATUS(runCounter NUMBER) AS JOB_STATUS; 
SELECT FN_COALESCE_API_RUN_JOB_STATUS(1452) AS JOB_STATUS; 

-- Run a job and return when complete
-- CALL COALESCE_API_RUN_JOB_COMPLETE(JobID NUMBER, EnvironmentID NUMBER)  
CALL COALESCE_API_RUN_JOB_COMPLETE(5, 3); 

-- Return a table of Job Execution runs
SELECT * FROM TABLE(COALESCE_API_RUNS_TABLE());

-- Return a table of Job step details
-- SELECT * FROM TABLE(COALESCE_API_RUN_DETAILS_TABLE(runCounter NUMBER)); 
SELECT * FROM TABLE(COALESCE_API_RUN_DETAILS_TABLE(1457));  

-- Run a job and send an email when complete
-- CALL RUN_JOB_SEND_EMAIL(JobID NUMBER, EnvironmentID NUMBER, Email_Recipients STRING)  
CALL RUN_JOB_SEND_EMAIL(5, 3, '<email address>');
