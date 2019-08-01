# Airflow REST API Plugin

### Description

A plugin for [Apache Airflow](https://github.com/apache/airflow) that exposes REST endpoints for the Command Line Interfaces listed in the airflow documentation:

[http://airflow.apache.org/cli.html](http://airflow.apache.org/cli.html)

The plugin also includes other custom REST APIs.

### System Requirements

* Airflow Versions
    * 1.X

### Deployment Instructions

1. Create the plugins folder if it doesn't exist. 

    * The location you should put it is usually at {AIRFLOW_HOME}/plugins. The specific location can be found in your airflow.cfg file:
    
        plugins_folder = /home/{USER_NAME}/airflow/plugins

2. Download the release code you want to deploy

    * Releases Available:
        * v0.0.1
        * v0.0.2
        * v1.0.0
        * v1.0.1
        * v1.0.2
        * v1.0.3
        * v1.0.4
        * v1.0.5
        * v1.0.6
        * v1.0.7
    * Branches Available:
        * master
        * v0.0.2-branch 
        * v1.0.0-branch 
        * v1.0.1-branch 
        * v1.0.2-branch 
        * v1.0.3-branch
        * v1.0.4-branch
        * v1.0.5-branch
        * v1.0.6-branch
        * v1.0.7-branch
    * URL to Download From:

        https://github.com/teamclairvoyant/airflow-rest-api-plugin/archive/{RELEASE_VERSION_OR_BRANCH_NAME}.zip
        
    * Note: Each release/branch has its own README.md file that describes the specific options and steps you should take to deploy and configure. Verify the options available in each release/branch after you download it.  

3. Unzip the file and move the contents of the plugins folder into your Airflow plugins directory
    
        unzip airflow-rest-api-plugin-{RELEASE_VERSION_OR_BRANCH_NAME}.zip
    
        cp -r airflow-rest-api-plugin-{RELEASE_VERSION_OR_BRANCH_NAME}/plugins/* {AIRFLOW_PLUGINS_FOLDER} 

4. Update the base_url configuration in the airflow.cfg file to the public host or ip of the machine the Airflow instance is running on (Optional)

    * This is required for the API admin page to display the correct host
    
5. (Optional) Append the following content to the end of the {AIRFLOW_HOME}/airflow.cfg file to give you control over execution:

        [rest_api_plugin]
        
        # Logs global variables used in the REST API plugin when the plugin is loaded. Set to False by default to avoid too many logging messages.
        # DEFAULT: False
        log_loading = False
        
        # Filters out loading messages from the standard out 
        # DEFAULT: True
        filter_loading_messages_in_cli_response = True
        
        # HTTP Header Name to be used for authenticating REST calls for the REST API Plugin
        # DEFAULT: 'rest_api_plugin_http_token'
        #rest_api_plugin_http_token_header_name = rest_api_plugin_http_token
           
        # HTTP Token  to be used for authenticating REST calls for the REST API Plugin
        # DEFAULT: None
        # Comment this out to disable Authentication
        #rest_api_plugin_expected_http_token = changeme

6. (Optional) Setup Authentication for Security

    a. Note: Requires that step #5 above be completed.

    b. Follow the "Enabling Authentication" section bellow.

7. Restart the Airflow Web Server

### Enabling Authentication

The REST API client supports a simple token based authentication mechanism where you can require users to pass in a specific http header to authenticate. By default this authentication mechanism is disabled but can be enabled with the "Setup" steps bellow. 

#### Setup

1. Edit your {AIRFLOW_HOME}/airflow.cfg file

    a. Under the [rest_api_plugin] section you added in step #5 of the "Deployment Instructions", uncomment the following configs:
    
        rest_api_plugin_http_token_header_name = rest_api_plugin_http_token
    
        rest_api_plugin_expected_http_token = changeme

2. (Optional) Update the 'rest_api_plugin_http_token_header_name' to the desired value 
        
3. Change the value of 'rest_api_plugin_expected_http_token' to the desired token people should pass 

4. Restart the Airflow Web Server

#### Authenticating

Once the steps above have been followed to enable authentication, users will need to pass a specific header along with their request to properly call the REST API. The header name is: rest_api_plugin_http_token

**Example CURL Command:**

curl --header "rest_api_plugin_http_token: changeme" http://{HOST}:{PORT}/admin/rest_api/api?api=version

#### What happens when you fail to Authenticate?

In the event that you have authentication enabled and the user calling the REST Endpoint doesn't include the header, you will get the following response:

    {
      "call_time": "{TIMESTAMP}",
      "http_response_code": 403,
      "output": "Token Authentication Failed",
      "response_time": "{TIMESTAMP}",
      "status": "ERROR"
    }

### Using the REST API

Once you deploy the plugin and restart the web server, you can start to use the REST API. Bellow you will see the endpoints that are supported. In addition, you can also interact with the REST API from the Airflow Web Server. When you reload the page, you will see a link under the Admin tab called "REST API". Clicking on the link will navigate you to the following URL:

http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/

This web page will show the Endpoints supported and provide a form for you to test submitting to them.
 

#### Endpoints

##### version

Gets the version of Airflow currently running. Supports both http GET and POST methods.

Available in Airflow Version: 1.0.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=version

Query Arguments:

None

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=version

##### rest_api_plugin_version

Displays the version of this REST API Plugin you're using.  Supports both http GET and POST methods.

Available in Airflow Version: None - Custom API

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=rest_api_plugin_version

Query Arguments:

None

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=rest_api_plugin_version

##### render

Render a task instance's template(s).  Supports both http GET and POST methods.

Available in Airflow Version: 1.7.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=render

Query Arguments:
    
* dag_id - string - The id of the dag
     
* task_id - string - The id of the task

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=render

http://{HOST}:{PORT}/admin/rest_api/api?api=render&dag_id=value&task_id=value&execution_date=2017-01-02T03:04:05&subdir=value

##### variables

CRUD operations on variables.  Supports both http GET and POST methods.

Available in Airflow Version: 1.7.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=variables

Query Arguments:

* set (optional) - Sets a variable. This requires passing the `cmd`, `key` and `value` parameters. Please see the example below.
    * cmd - string - Only allowed value is `cmd=set`
    * key - string - name of the variable
    * value - string - value of the variable
    
* get (optional) - string - Get value of a variable
     
* json (optional) - boolean - Deserialize JSON variable

* default (optional) - string - Default value returned if variable does not exist

* import (optional) - string - Import variables from JSON file

* export (optional) - string - Export variables from JSON file

* delete (optional) - string - Delete a variable

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=variables

http://{HOST}:{PORT}/admin/rest_api/api?api=variables&cmd=set&key=value&value=value&get=value&json&default=value&import=value&export=value&delete=value

For setting a variable like `myVar1=myValue1` use

`http://{HOST}:{PORT}/admin/rest_api/api?api=variables&cmd=set&key=myVar1&value=myValue1` 

##### connections

List/Add/Delete connections.  Supports both http GET and POST methods.

Available in Airflow Version: 1.8.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=connections

Query Arguments:

* list (optional) - boolean - List all connections

* add (optional) - string - Add a connection

* delete (optional) - boolean - Delete a connection

* conn_id (optional) - string - Connection id, required to add/delete a connection

* conn_uri (optional) - string - Connection URI, required to add a connection

* conn_extra (optional) - string - Connection 'Extra' field, optional when adding a connection

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=connections

##### pause

Pauses a DAG.  Supports both http GET and POST methods.

Available in Airflow Version: 1.7.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=pause

Query Arguments:

* dag_id - string - The id of the dag

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=pause&dag_id=test_id

##### unpause

Resume a paused DAG.  Supports both http GET and POST methods.

Available in Airflow Version: 1.7.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=unpause

Query Arguments:

* dag_id - string - The id of the dag

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=unpause&dag_id=test_id

##### task_failed_deps

Returns the unmet dependencies for a task instance from the perspective of the scheduler. In other words, why a task instance doesn't get scheduled and then queued by the scheduler, and then run by an executor).  Supports both http GET and POST methods.

Available in Airflow Version: 1.8.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=task_failed_deps

Query Arguments:

* dag_id - string - The id of the dag
     
* task_id - string - The id of the task

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=task_failed_deps&dag_id=value&task_id=value&execution_date=2017-01-02T03:04:05

##### trigger_dag

Triggers a Dag to Run.  Supports both http GET and POST methods.

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=trigger_dag

Available in Airflow Version: 1.6.0 or greater

Query Arguments:
    
* dag_id - The DAG ID of the DAG you want to trigger
     
* run_id (Optional) - The RUN ID to use for the DAG run

* conf (Optional) - Some configuration to pass to the DAG you trigger - (URL Encoded JSON)

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=trigger_dag&dag_id=test_id

http://{HOST}:{PORT}/admin/rest_api/api?api=trigger_dag&dag_id=test_id&run_id=run_id_2016_01_01&conf=%7B%22key%22%3A%22value%22%7D

##### test

Test a task instance. This will run a task without checking for dependencies or recording it's state in the database.

Available in Airflow Version: 0.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=test

Query Arguments:

* dag_id - string - The id of the dag
     
* task_id - string - The id of the task

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

* dryrun (optional) - boolean - Perform a dry run

* task_params (optional) - string - Sends a JSON params dict to the task

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=test&dag_id=value&task_id=value&execution_date=2017-01-02T03:04:05

##### dag_state

Get the status of a dag run.  Supports both http GET and POST methods.

Available in Airflow Version: 1.8.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=dag_state

Query Arguments:

* dag_id - string - The id of the dag

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=dag_state&dag_id=test_id&execution_date=2017-01-02T03:04:05

##### run

Run a single task instance. Supports both http GET and POST methods.

Available in Airflow Version: 1.0.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=run

Query Arguments:

* dag_id - string - The id of the dag
     
* task_id - string - The id of the task

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

* mark_success (optional) - boolean - Mark jobs as succeeded without running them

* force (optional) - boolean - Ignore previous task instance state, rerun regardless if task already succeede

* pool (optional) - boolean - Resource pool to use

* cfg_path (optional) - boolean - Path to config file to use instead of airflow.cfg

* local (optional) - boolean - Run the task using the LocalExecutor

* ignore_all_dependencies (optional) - boolean - Ignores all non-critical dependencies, including ignore_ti_state and ignore_task_depsstore_true

* ignore_dependencies (optional) - boolean - Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies

* ignore_depends_on_past (optional) - boolean - Ignore depends_on_past dependencies (but respect upstream dependencies)

* ship_dag (optional) - boolean - Pickles (serializes) the DAG and ships it to the worker

* pickle (optional) - string - Serialized pickle object of the entire dag (used internally)

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=run&dag_id=value&task_id=value&execution_date=2017-01-02T03:04:05

##### list_tasks

List the tasks within a DAG. Supports both http GET and POST methods.

Available in Airflow Version: 0.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=list_tasks

Query Arguments:

* dag_id - string - The id of the dag
     
* tree (optional) - boolean - Tree view

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=list_tasks&dag_id=test_id

##### backfill

Run subsections of a DAG for a specified date range. Supports both http GET and POST methods.

Available in Airflow Version: 0.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=backfill

Query Arguments:

* dag_id - string - The id of the dag

* task_regex (optional) - string - The regex to filter specific task_ids to backfill (optional)

* start_date (optional) - string - Override start_date YYYY-MM-DD. Either this or the end_date needs to be provided.

* end_date (optional) - string - Override end_date YYYY-MM-DD. Either this or the start_date needs to be provided.

* mark_success (optional) - boolean - Mark jobs as succeeded without running them

* local (optional) - boolean - Run the task using the LocalExecutor

* donot_pickle (optional) - boolean - Do not attempt to pickle the DAG object to send over to the workers, just tell the workers to run their version of the code.

* include_adhoc (optional) - boolean - Include dags with the adhoc argument.

* ignore_dependencies (optional) - boolean - Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies

* ignore_first_depends_on_past (optional) - boolean - Ignores depends_on_past dependencies for the first set of tasks only (subsequent executions in the backfill DO respect depends_on_past)

* subdir (optional) - string - File location or directory from which to look for the dag

* pool (optional) - string - Resource pool to use

* dry_run (optional) - boolean - Perform a dry run

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=backfill&dag_id=test_id

##### list_dags

List all the DAGs. Supports both http GET and POST methods.

Available in Airflow Version: 0.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=list_dags

Query Arguments:

* subdir (optional) - string - File location or directory from which to look for the dag

* report (optional) - boolean - Show DagBag loading report

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=list_dags

##### kerberos

Start a kerberos ticket renewer. Supports both http GET and POST methods.

Available in Airflow Version: 1.6.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=kerberos

Query Arguments:

* principal (optional) - string - kerberos principal

* keytab (optional) - string - keytab

* pid (optional) - string - PID file location

* daemon (optional) - boolean - Daemonize instead of running in the foreground

* stdout (optional) - string - Redirect stdout to this file

* stderr (optional) - string - Redirect stderr to this file

* log-file (optional) - string - Location of the log file

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=kerberos

##### worker

Start a Celery worker node. Supports both http GET and POST methods.

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=worker

Available in Airflow Version: 0.1 or greater

Query Arguments:

* do_pickle (optional) - boolean - Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.

* queues (optional) - string - Comma delimited list of queues to serve

* concurrency (optional) - string - The number of worker processes

* pid (optional) - string - PID file location

* daemon (optional) - boolean - Daemonize instead of running in the foreground

* stdout (optional) - string - Redirect stdout to this file

* stderr (optional) - string - Redirect stderr to this file

* log-file (optional) - string - Location of the log file

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=worker

##### flower

Start a Celery worker node. Supports both http GET and POST methods.

Available in Airflow Version: 1.0.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=flower

Query Arguments:

* hostname (optional) - string - Set the hostname on which to run the server

* port (optional) - string - The port on which to run the server

* flower_conf (optional) - string - Configuration file for flower

* broker_api (optional) - string - Broker api

* pid (optional) - string - PID file location

* daemon (optional) - boolean - Daemonize instead of running in the foreground

* stdout (optional) - string - Redirect stdout to this file

* stderr (optional) - string - Redirect stderr to this file

* log-file (optional) - string - Location of the log file

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=flower

##### scheduler

Start a scheduler instance. Supports both http GET and POST methods.

Available in Airflow Version: 1.0.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=scheduler

Query Arguments:

* dag_id (optional) - string - The id of the dag

* subdir (optional) - string - File location or directory from which to look for the dag

* run-duration (optional) - string - Set number of seconds to execute before exiting  

* num_runs (optional) - string -  Set the number of runs to execute before exiting

* do_pickle (optional) - boolean - Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.

* pid (optional) - string - PID file location

* daemon (optional) - boolean - Daemonize instead of running in the foreground

* stdout (optional) - string - Redirect stdout to this file

* stderr (optional) - string - Redirect stderr to this file

* log-file (optional) - string - Location of the log file

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=scheduler

##### task_state

Get the status of a task instance. Supports both http GET and POST methods.

Available in Airflow Version: 1.0.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=task_state

Query Arguments:

* dag_id - string - The id of the dag
     
* task_id - string - The id of the task

* execution_date - string - The execution date of the DAG (Example: 2017-01-02T03:04:05)

* subdir (optional) - string - File location or directory from which to look for the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=task_state&dag_id=value&task_id=value&execution_date=2017-01-02T03:04:05

##### pool

CRUD operations on pools. Supports both http GET and POST methods.

Available in Airflow Version: 1.8.0 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=pool

Query Arguments:

* set (optional) - Set pool name, slot count and description respectively. This requires passing the `cmd`, `pool_name`, `slot_count` and `description` parameters. Please see the example below.
    * cmd - string - Only allowed value is `cmd=set`
    * pool_name - string - name of the Pool
    * slot_count - number - no.of slots in the Pool
    * description - string - description of the Pool

* get (optional) - string - Get pool info

* delete (optional) - string - Delete a pool

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=pool

http://{HOST}:{PORT}/admin/rest_api/api?api=pool&cmd=set&pool_name=value&slot_count=value&description=value&get=value&delete=value

For setting a `myTestpool` with a slot count of `10` and with `myTestpoolDescription` description.

`http://{HOST}:{PORT}/admin/rest_api/api?api=pool&cmd=set&pool_name=myTestpool&slot_count=10&description=myTestpoolDescription`


##### serve_logs

Serve logs generate by worker. Supports both http GET and POST methods.

Available in Airflow Version: 0.1 or greater

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=serve_logs

Query Arguments:

None

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=serve_logs

##### clear

Clear a set of task instance, as if they never ran. Supports both http GET and POST methods.

Available in Airflow Version: 0.1 or greater 

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=clear

Query Arguments:

* dag_id - string - The id of the dag

* task_regex (optional) - string - The regex to filter specific task_ids to backfill (optional)

* start_date (optional) - string - Override start_date YYYY-MM-DD

* end_date (optional) - string - Override end_date YYYY-MM-DD

* subdir (optional) - string - File location or directory from which to look for the dag

* upstream (optional) - boolean - Include upstream tasks

* downstream (optional) - boolean - Include downstream tasks

* no_confirm (optional) - boolean - Do not request confirmation

* only_failed (optional) - boolean - Only failed jobs

* only_running (optional) - boolean - Only running jobs

* exclude_subdags (optional) - boolean - Exclude subdags

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=clear

##### deploy_dag

Deploy a new DAG. Supports both http GET and POST methods.

Available in Airflow Version: None - Custom API

POST - http://{HOST}:{PORT}/admin/rest_api/api?api=deploy_dag

POST Body Arguments:

* dag_file - file - Python file to upload and deploy

* force (optional) - boolean - Whether to forcefully upload the file if the file already exists or not

* pause (optional) - boolean - The DAG will be forced to be paused when created and override the 'dags_are_paused_at_creation' config.

* unpause (optional) - boolean - The DAG will be forced to be unpaused when created and override the 'dags_are_paused_at_creation' config.

Examples:

Header: multipart/form-data

URL: http://{HOST}:{PORT}/admin/rest_api/api?api=deploy_dag

Body: dag_file=path_to_file&force=on

CURL Example:

curl -X POST -H 'Content-Type: multipart/form-data' -F 'dag_file=@/path/to/dag.py' -F 'force=on' http://{HOST}:{PORT}/admin/rest_api/api?api=deploy_dag

##### refresh_dag

Refresh a DAG. Supports both http GET and POST methods.

Available in Airflow Version: None - Custom API

GET - http://{HOST}:{PORT}/admin/rest_api/api?api=refresh_dag

Query Arguments:

* dag_id - string - The id of the dag

Examples:

http://{HOST}:{PORT}/admin/rest_api/api?api=refresh_dag&dag_id=test_id

#### API Response

The API's will all return a common response object. It is a JSON object with the following entries in it:

* airflow_cmd           - String    - Airflow CLI command being ran on the local machine
* arguments             - Dict      - Dictionary with the arguments you passed in and their values
* post_arguments        - Dict      - Dictionary with the post body arguments you passed in and their values
* call_time             - Timestamp - Time in which the request was received by the server 
* output                - String    - Text output from calling the CLI function
* response_time         - Timestamp - Time in which the response was sent back by the server 
* status                - String    - Response Status of the call. (possible values: OK, ERROR)
* warning               - String    - A Warning message that's sent back from the API 
* http_response_code    - Integer   - HTTP Response code 

**Sample** (Result of calling the versions endpoint)

    {
      "airflow_cmd": "airflow version",
      "arguments": {},
      "call_time": "Tue, 29 Nov 2016 14:22:26 GMT",
      "http_response_code": 200,
      "output": "1.7.0",
      "response_time": "Tue, 29 Nov 2016 14:27:59 GMT",
      "status": "OK"
    }
