-- Set up secrets and integrations

-- Create a network rule for Snowflake to reach Coalesce API

CREATE OR REPLACE  NETWORK RULE COALESCE_API_RULE
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('APP.COALESCESOFTWARE.IO');

-- Create a Secret containing Token from Coalesce

CREATE OR REPLACE SECRET COALESCE_API_TOKEN 
TYPE = GENERIC_STRING 
SECRET_STRING='<your Coalesce token>';
GRANT USAGE ON SECRET COALESCE_API_TOKEN TO <role>;

-- Create a Secret containing Snowflake User name and password for Service account that run a job
    
CREATE OR REPLACE SECRET COALESCE_API_USER_PW
TYPE = password
USERNAME = '<your Snowflake service account name>'
PASSWORD = '<your Snowflake service account password>';
GRANT USAGE ON SECRET COALESCE_API_USER_PW TO <role>;

-- Create Integration to access Coalesce API

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION COALESCE_API_INTEGRATION
ALLOWED_NETWORK_RULES = (COALESCE_API_RULE)
ALLOWED_AUTHENTICATION_SECRETS = (COALESCE_API_TOKEN, COALESCE_API_USER_PW)
ENABLED=TRUE;

– Optionally to send emails setup an Email Integration (https://docs.snowflake.com/en/user-guide/email-stored-procedures) 
ALTER USER <user> SET EMAIL = '<email address>';

CREATE NOTIFICATION INTEGRATION COALESCE_EMAIL
    TYPE=email
    ENABLED=true
    ALLOWED_RECIPIENTS=('<email address1>', '<email address2>');
