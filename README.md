# Airflow REST API Plugin

### Description

A plugin for Apache Airflow (https://github.com/apache/incubator-airflow) that exposes REST endpoints for the Command Line Interfaces listed in the airflow documentation:

http://airflow.incubator.apache.org/cli.html

Also includes other useful REST APIs.

### Deployment Instructions

1. Create the plugins folder if it doesn't exist. 

    * The location you should put it is usually at {AIRFLOW_HOME}/plugins. The specific location can be found in your airflow.cfg file:
    
        plugins_folder = /home/{USER_NAME}/airflow/plugins
    
2. Copy the contents of the plugins folder from this repo into the plugins folder you created on the Airflow server.

3. Setup Authentication for Security (Optional)

    a. Follow the "Enabling Authentication" section bellow.

4. Restart the Airflow Web Server

### Enabling Authentication

The REST API client supports a simple token based authentication mechanism where you can require users to pass in a specific http header to authenticate. By default this authentication mechanism is disabled but can be enabled with the "Setup" steps bellow. 

#### Setup

1. Edit your {AIRFLOW_HOME}/airflow.cfg file

    a. Under the [webserver] section add the following content:
    
        # HTTP Token to be used for authenticating REST calls for the REST API Plugin
        # Comment this out to disable Authentication
        rest_api_plugin_expected_http_token = {HTTP_TOKEN_PLACEHOLDER}
        
2. Fill in the {HTTP_TOKEN_PLACEHOLDER} with your desired token people should pass 

3. Restart the Airflow Web Server

#### Authenticating

Once the steps above have been followed to enable authentication, users will need to pass a specific header along with their request to properly call the REST API. The header name is: rest_api_plugin_http_token

**Example CURL Command:**

curl --header "rest_api_plugin_http_token: {HTTP_TOKEN_PLACEHOLDER}" http://{HOST}:{PORT}/admin/rest_api/api/v1.0/version

#### What happens when you fail to Authenticate?

In the event that you have authentication enabled and the user calling the REST Endpoint doesn't include the header, you will get the following response:

{
  "call_time": "{TIMESTAMP}",
  "output": "Token Authentication Failed",
  "response_time": "{TIMESTAMP}",
  "status": "ERROR"
}

### Using the REST API

Once you deploy the plugin and restart the web server, you can start to use the REST API. Bellow you will see the endpoints that are supported. In addition, you can also interact with the REST API from the Airflow Web Server. When you reload the page, you will see a link under the Admin tab called "REST API". Clicking on the link will navigate you to the following URL:

http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/

This web page will show the Endpoints supported and provide a form for you to test submitting to them.
 

#### Endpoints

###### Version

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/version

Gets the version of Airflow currently running

Query Arguments:

None

Examples:

http://{HOST}:{PORT}/admin/rest_api/api/v1.0/version

###### Variables

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/variables

Displays the version of Airflow you're using

TODO: FILL OUT

###### Pause

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/pause

Pauses a DAG

TODO: FILL OUT

###### Unpause

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/unpause

Resume a paused DAG

TODO: FILL OUT

###### Test

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/test

Test a task instance. This will run a task without checking for dependencies or recording it’s state in the database.

TODO: FILL OUT

###### DAG State

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/dag_state

Get the status of a dag run

TODO: FILL OUT

###### Run

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/run

Run a single task instance

TODO: FILL OUT

###### List Tasks

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/list_tasks

List the tasks within a DAG

TODO: FILL OUT

###### Backfill DAG

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/backfill

Run subsections of a DAG for a specified date range

TODO: FILL OUT

###### List DAGs

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/list_dags

List all the DAGs

TODO: FILL OUT

###### Kerberos

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/kerberos

Start a kerberos ticket renewer

TODO: FILL OUT

###### Workers

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/workers

Start a Celery worker node

TODO: FILL OUT

###### Scheduler

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/variables

Start a scheduler instance

TODO: FILL OUT

###### Trigger DAG

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/trigger_dag

Triggers a Dag to Run

Query Arguments:
    
* dag_id - The DAG ID of the DAG you want to trigger
     
* run_id (Optional) - The RUN ID to use for the DAG run

* conf (Optional) - Some configuration to pass to the DAG you trigger - (URL Encoded JSON)

Examples:

http://{HOST}:{PORT}/admin/rest_api/api/v1.0/trigger_dag?dag_id=test_id

http://{HOST}:{PORT}/admin/rest_api/api/v1.0/trigger_dag?dag_id=test_id&run_id=run_id_2016_01_01&conf=%7B%22key%22%3A%22value%22%7D

###### Refresh DAG

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/refresh_dag

Refresh a DAG

TODO: FILL OUT

###### Deploy DAG

GET - http://{HOST}:{PORT}/admin/rest_api/api/v1.0/deploy_dag

Deploy a new DAG

TODO: FILL OUT

#### API Response

The API's will all return a common response object. It is a JSON object with the following entries in it:

arguments      - Dict       - Dictionary with the arguments you passed in and their values
call_time      - Timestamp  - Time in which the request was received by the server 
output         - String     - Text output from calling the CLI function
response_time  - Timestamp  - Time in which the response was sent back by the server 
status         - String     - Response Status of the call. (possible values: OK, ERROR)

**Sample** (Result of calling the versions endpoint)

{
  "arguments": {},
  "call_time": "Tue, 29 Nov 2016 14:22:26 GMT",
  "output": "1.7.0",
  "response_time": "Tue, 29 Nov 2016 14:27:59 GMT",
  "status": "OK"
}
