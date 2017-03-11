from airflow.models import DagBag, DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow import configuration
from airflow.www.app import csrf

from flask import Blueprint, request, jsonify
from flask_admin import BaseView, expose

from datetime import datetime
import airflow
import logging
import subprocess
import urllib2
import os

"""
CLIs this REST API exposes are Defined here: http://airflow.incubator.apache.org/cli.html
"""

# todo: test functions
# todo: add check to see if dag exists
# todo: improve logging

rest_api_endpoint = "/admin/rest_api/api"
filter_loading_messages_in_cli_response = True
airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
airflow_base_log_folder = configuration.get('core', 'BASE_LOG_FOLDER')
airflow_dags_folder = configuration.get('core', 'DAGS_FOLDER')
airflow_rest_api_plugin_http_token_header_name = "rest_api_plugin_http_token"
airflow_expected_http_token = None
if configuration.has_option("webserver", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN"):
    airflow_expected_http_token = configuration.get("webserver", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN")

"""
API OBJECT:
{
    "name": "{string}",                 # TBC
    "description": "{string}",          # TBC
    "airflow_version": "{string}",      # TBC
    "http_method": "{string}",          # TBC
    "background_mode": {boolean},       # TBC (optional)
    "arguments": [                      # TBC
        {
            "name": "{string}",         # TBC
            "description": "{string}",  # TBC
            "form_type": "{string}",    # TBC
            "required": {boolean},      # TBC
            "cli_end_position": {int}   # TBC (optional)
        }
    ]
},
"""

apis = [
    {
        "name": "version",
        "description": "Displays the version of Airflow you're using",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": []
    },
    {
        "name": "render",
        "description": "Render a task instance's template(s)",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "variables",
        "description": "CRUD operations on variables",
        "airflow_version": "1.7.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "set", "description": "Set a variable", "form_type": "text", "required": False},
            {"name": "get", "description": "Get value of a variable", "form_type": "text", "required": False},
            {"name": "json", "description": "Deserialize JSON variable", "form_type": "checkbox", "required": False},
            {"name": "default", "description": "Default value returned if variable does not exist", "form_type": "text", "required": False},
            {"name": "import", "description": "Import variables from JSON file", "form_type": "text", "required": False},
            {"name": "export", "description": "Export variables to JSON file", "form_type": "text", "required": False},
            {"name": "delete", "description": "Delete a variable", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "connections",
        "description": "List/Add/Delete connections",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "list", "description": "List all connections", "form_type": "checkbox", "required": False},
            {"name": "add", "description": "Add a connection", "form_type": "checkbox", "required": False},
            {"name": "delete", "description": "Delete a connection", "form_type": "checkbox", "required": False},
            {"name": "conn_id", "description": "Connection id, required to add/delete a connection", "form_type": "text", "required": False},
            {"name": "conn_uri", "description": "Connection URI, required to add a connection", "form_type": "text", "required": False},
            {"name": "conn_extra", "description": "Connection 'Extra' field, optional when adding a connection", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "pause",
        "description": "Pauses a DAG",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "unpause",
        "description": "Unpauses a DAG",
        "airflow_version": "1.7.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "task_failed_deps",
        "description": "Returns the unmet dependencies for a task instance from the perspective of the scheduler. In other words, why a task instance doesn't get scheduled and then queued by the scheduler, and then run by an executor).",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "trigger_dag",
        "description": "Trigger a DAG run",
        "airflow_version": "1.6.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "run_id", "description": "Helps to identify this run", "form_type": "text", "required": False},
            {"name": "conf", "description": "JSON string that gets pickled into the DagRun's conf attribute", "form_type": "text", "required": False},
            {"name": "exec_date", "description": "The execution date of the DAG", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "test",
        "description": "Test a task instance. This will run a task without checking for dependencies or recording it's state in the database.",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "dry_run", "description": "Perform a dry run", "form_type": "checkbox", "required": False},
            {"name": "task_params", "description": "Sends a JSON params dict to the task", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "dag_state",
        "description": "Get the status of a dag run",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "run",
        "description": "Run a single task instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "mark_success", "description": "Mark jobs as succeeded without running them", "form_type": "checkbox", "required": False},
            {"name": "force", "description": "Ignore previous task instance state, rerun regardless if task already succeede", "form_type": "checkbox", "required": False},
            {"name": "pool", "description": "Resource pool to use", "form_type": "text", "required": False},
            {"name": "cfg_path", "description": "Path to config file to use instead of airflow.cfg", "form_type": "text", "required": False},
            {"name": "local", "description": "Run the task using the LocalExecutor", "form_type": "checkbox", "required": False},
            {"name": "ignore_all_dependencies", "description": "Ignores all non-critical dependencies, including ignore_ti_state and ignore_task_depsstore_true", "form_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "description": "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies", "form_type": "checkbox", "required": False},
            {"name": "ignore_depends_on_past", "description": "Ignore depends_on_past dependencies (but respect upstream dependencies)", "form_type": "checkbox", "required": False},
            {"name": "ship_dag", "description": "Pickles (serializes) the DAG and ships it to the worker", "form_type": "checkbox", "required": False},
            {"name": "pickle", "description": "Serialized pickle object of the entire dag (used internally)", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "list_tasks",
        "description": "List the tasks within a DAG",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "tree", "description": "Tree view", "form_type": "checkbox", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "backfill",
        "description": "Run subsections of a DAG for a specified date range",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "task_regex", "description": "The regex to filter specific task_ids to backfill (optional)", "form_type": "text", "required": False},
            {"name": "start_date", "description": "Override start_date YYYY-MM-DD", "form_type": "text", "required": False},
            {"name": "end_date", "description": "Override end_date YYYY-MM-DD", "form_type": "text", "required": False},
            {"name": "mark_success", "description": "Mark jobs as succeeded without running them", "form_type": "checkbox", "required": False},
            {"name": "local", "description": "Run the task using the LocalExecutor", "form_type": "checkbox", "required": False},
            {"name": "donot_pickle", "description": "Do not attempt to pickle the DAG object to send over to the workers, just tell the workers to run their version of the code.", "form_type": "checkbox", "required": False},
            {"name": "include_adhoc", "description": "Include dags with the adhoc argument.", "form_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "description": "Ignore task-specific dependencies, e.g. upstream, depends_on_past, and retry delay dependencies", "form_type": "checkbox", "required": False},
            {"name": "ignore_first_depends_on_past", "description": "Ignores depends_on_past dependencies for the first set of tasks only (subsequent executions in the backfill DO respect depends_on_past).", "form_type": "checkbox", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "pool", "description": "Resource pool to use", "form_type": "text", "required": False},
            {"name": "dry_run", "description": "Perform a dry run", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "list_dags",
        "description": "List all the DAGs",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "report", "description": "Show DagBag loading report", "form_type": "checkbox", "required": False}
        ]
    },
    {  # todo: test
        "name": "kerberos",
        "description": "Start a kerberos ticket renewer",
        "airflow_version": "1.6.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "principal", "description": "kerberos principal", "form_type": "text", "required": True},
            {"name": "keytab", "description": "keytab", "form_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_type": "text", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_type": "text", "required": False},
        ]
    },
    {  # todo: test
        "name": "worker",
        "description": "Start a Celery worker node",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "do_pickle", "description": "Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.", "form_type": "checkbox", "required": False},
            {"name": "queues", "description": "Comma delimited list of queues to serve", "form_type": "text", "required": False},
            {"name": "concurrency", "description": "The number of worker processes", "form_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_type": "checkbox", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "flower",
        "description": "Start a Celery worker node",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "hostname", "description": "Set the hostname on which to run the server", "form_type": "text", "required": False},
            {"name": "port", "description": "The port on which to run the server", "form_type": "text", "required": False},
            {"name": "flower_conf", "description": "Configuration file for flower", "form_type": "text", "required": False},
            {"name": "broker_api", "description": "Broker api", "form_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_type": "text", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "scheduler",
        "description": "Start a scheduler instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": False},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "run-duration", "description": "Set number of seconds to execute before exiting", "form_type": "text", "required": False},
            {"name": "num_runs", "description": "Set the number of runs to execute before exiting", "form_type": "text", "required": False},
            {"name": "do_pickle", "description": "Attempt to pickle the DAG object to send over to the workers, instead of letting workers run their version of the code.", "form_type": "text", "required": False},
            {"name": "pid", "description": "PID file location", "form_type": "checkbox", "required": False},
            {"name": "daemon", "description": "Daemonize instead of running in the foreground", "form_type": "checkbox", "required": False},
            {"name": "stdout", "description": "Redirect stdout to this file", "form_type": "text", "required": False},
            {"name": "stderr", "description": "Redirect stderr to this file", "form_type": "text", "required": False},
            {"name": "log-file", "description": "Location of the log file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "task_state",
        "description": "Get the status of a task instance",
        "airflow_version": "1.0.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "description": "The id of the task", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "description": "The execution date of the DAG", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False}
        ]
    },
    {  # todo: test
        "name": "pool",
        "description": "CRUD operations on pools",
        "airflow_version": "1.8.0 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "set", "description": "Set pool slot count and description, respectively", "form_type": "checkbox", "required": False},
            {"name": "get", "description": "Get pool info", "form_type": "text", "required": False},
            {"name": "delete", "description": "Delete a pool", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "serve_logs",
        "description": "Serve logs generate by worker",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "background_mode": True,
        "arguments": []
    },
    {  # todo: test
        "name": "clear",
        "description": "Clear a set of task instance, as if they never ran",
        "airflow_version": "0.1 or greater",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "description": "File location or directory from which to look for the dag", "form_type": "text", "required": False},
            {"name": "task_regex", "description": "The regex to filter specific task_ids to backfill (optional)", "form_type": "text", "required": False},
            {"name": "start_date", "description": "Override start_date YYYY-MM-DD", "form_type": "text", "required": False},
            {"name": "end_date", "description": "Override end_date YYYY-MM-DD", "form_type": "text", "required": False},
            {"name": "upstream", "description": "Include upstream tasks", "form_type": "checkbox", "required": False},
            {"name": "downstream", "description": "Include downstream tasks", "form_type": "checkbox", "required": False},
            {"name": "no_confirm", "description": "Do not request confirmation", "form_type": "checkbox", "required": False},
            {"name": "only_failed", "description": "Only failed jobs", "form_type": "checkbox", "required": False},
            {"name": "only_running", "description": "Only running jobs", "form_type": "checkbox", "required": False},
            {"name": "exclude_subdags", "description": "Exclude subdags", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "deploy_dag",
        "description": "Deploy a new DAG File to the DAGs directory",
        "airflow_version": "None - Custom API",
        "http_method": "POST",
        "post_body_description": "dag_file - POST Body Element - REQUIRED",
        "form_enctype": "multipart/form-data",
        "arguments": [],
        "post_arguments": [
            {"name": "dag_file", "description": "Python file to upload and deploy", "form_type": "file", "required": True}
        ]
    },
    {
        "name": "refresh_dag",
        "description": "Refresh a DAG in the Web Server",
        "airflow_version": "None - Custom API",
        "http_method": "GET",
        "arguments": [
            {"name": "dag_id", "description": "The id of the dag", "form_type": "text", "required": True}
        ]
    }
]


def get_base_response(status="OK", call_time=datetime.now(), include_arguments=True):
    base_response = {"status": status, "call_time": call_time}
    if include_arguments:
        base_response["arguments"] = request.args
    return base_response


def get_final_response(base_response, output=None, airflow_cmd=None):
    final_response = base_response
    final_response["response_time"] = datetime.now()
    if output:
        final_response["output"] = output
    if airflow_cmd:
        final_response["airflow_cmd"] = airflow_cmd
    return jsonify(final_response)


def get_error_response(base_response, error_code, output=None):
    base_response["status"] = "ERROR"
    return get_final_response(base_response=base_response, output=output), error_code


def get_403_error_response(base_response, output=None):
    return get_error_response(base_response, 403, output)


def get_400_error_response(base_response, output=None):
    return get_error_response(base_response, 400, output)


def is_arg_not_provided(arg):
    return arg is None or arg == ""


def http_token_secure(func):
    def secure_check(arg):
        logging.info("Rest_API_Plugin.http_token_secure() func: " + str(func))
        if airflow_expected_http_token:
            logging.info("Performing Token Authentication")
            if request.headers.get(airflow_rest_api_plugin_http_token_header_name, None) != airflow_expected_http_token:
                logging.warn("Token Authentication Failed")
                base_response = get_base_response(include_arguments=False)
                return get_403_error_response(base_response=base_response, output="Token Authentication Failed")
        return func(arg)

    return secure_check


class REST_API(BaseView):

    @expose('/')
    def index(self):
        logging.info("REST_API.index() called")
        dagbag = DagBag()
        dags = []
        for dag_id in dagbag.dags:
            orm_dag = DagModel.get_current(dag_id)
            dags.append({"dag_id": dag_id, "is_active": not orm_dag.is_paused})

        return self.render("rest_api_plugin/index.html",
                           dags=dags,
                           airflow_webserver_base_url=airflow_webserver_base_url,
                           rest_api_endpoint=rest_api_endpoint,
                           apis=apis
                           )

    @csrf.exempt
    @expose('/api', methods=["GET", "POST"])
    @http_token_secure
    def api(self):
        logging.info("REST_API.api() called")
        api = request.args.get('api')
        if api is not None:
            api = api.strip()
        logging.info("api: " + str(api))
        base_response = get_base_response()

        if is_arg_not_provided(api):
            return get_400_error_response(base_response, "api should be provided")

        api_object = None
        for api_object_to_check in apis:
            if api_object_to_check["name"] == api:
                api_object = api_object_to_check
        if api_object is None:
            return get_400_error_response(base_response, "api '" + str(api) + "' was not found")

        missing_required_arguments = []
        for argument in api_object["arguments"]:
            argument_name = argument["name"]
            argument_value = request.args.get(argument_name)
            if argument["required"]:
                if is_arg_not_provided(argument_value):
                    missing_required_arguments.append(argument_name)
        if len(missing_required_arguments) > 0:
            return get_400_error_response(base_response, "The argument(s) " + str(missing_required_arguments) + " are required")

        if api == "version":
            final_response = self.version(base_response)
        elif api == "deploy_dag":
            final_response = self.deploy_dag(base_response)
        elif api == "refresh_dag":
            final_response = self.refresh_dag(base_response)
        else:
            final_response = self.execute_cli(base_response, api_object)

        return final_response

    def execute_cli(self, base_response, api_object):
        logging.info("Executing cli function")

        largest_end_argument_value = 0
        for argument in api_object["arguments"]:
            if argument.get("cli_end_position") is not None and argument["cli_end_position"] > largest_end_argument_value:
                largest_end_argument_value = argument["cli_end_position"]

        airflow_cmd_split = ["airflow", api_object["name"]]
        end_arguments = [0] * largest_end_argument_value
        for argument in api_object["arguments"]:
            argument_name = argument["name"]
            argument_value = request.args.get(argument_name)
            logging.info("argument_name: " + str(argument_name) + ", argument_value: " + str(argument_value))
            if argument_value is not None:
                if "cli_end_position" in argument:
                    logging.info("argument['cli_end_position']: " + str(argument['cli_end_position']))
                    end_arguments[argument["cli_end_position"]-1] = argument_value
                else:
                    if argument["form_type"] == "checkbox":
                        airflow_cmd_split.extend(["--" + argument_name])
                    else:
                        airflow_cmd_split.extend(["--" + argument_name, argument_value])
            else:
                logging.warning("argument_value is null")

        airflow_cmd_split.extend(end_arguments)

        if "background_mode" in api_object and api_object["background_mode"]:
            if request.args.get("log-file") is None:
                airflow_cmd_split.append("> " + str(airflow_base_log_folder) + "/" + api_object["name"] + ".log")
            airflow_cmd_split.append("&")

        airflow_cmd = " ".join(airflow_cmd_split)
        logging.info("airflow_cmd array: " + str(airflow_cmd_split))

        if "background_mode" in api_object and api_object["background_mode"]:
            output = self.execute_cli_command_background_mode(airflow_cmd)
        else:
            output = self.execute_cli_command(airflow_cmd_split)

        if filter_loading_messages_in_cli_response:
            output = self.filter_loading_messages(output)

        return get_final_response(base_response=base_response, output=output, airflow_cmd=airflow_cmd)

    def version(self, base_response):
        logging.info("Executing custom version function")
        return get_final_response(base_response, airflow.__version__)

    def deploy_dag(self, base_response):
        logging.info("Executing custom deploy_dag function")
        # check if the post request has the file part
        if 'dag_file' not in request.files:
            return get_400_error_response(base_response, "dag_file should be provided")
        dag_file = request.files['dag_file']
        # if user does not select file, browser also submits an empty part without filename
        if dag_file.filename == '':
            return get_400_error_response(base_response, "dag_file should be provided")
        if dag_file and dag_file.filename.endswith(".py"):
            dag_file.save(os.path.join(airflow_dags_folder, dag_file.filename))
        else:
            return get_400_error_response(base_response, "dag_file is not a *.py file")
        return get_final_response(base_response=base_response, output="DAG File [{}] has been uploaded".format(dag_file))

    def refresh_dag(self, base_response):
        logging.info("Executing custom refresh_dag function")
        dag_id = request.args.get('dag_id')

        if is_arg_not_provided(dag_id):
            return get_400_error_response(base_response, "dag_id should be provided")
        elif " " in dag_id:
            return get_400_error_response(base_response, "dag_id contains spaces and is therefore an illegal argument")

        refresh_dag_url = str(airflow_webserver_base_url) + '/admin/airflow/refresh?dag_id=' + str(dag_id)
        logging.info("Calling: " + str(refresh_dag_url))
        response = urllib2.urlopen(refresh_dag_url)
        html = response.read()  # avoid using this as the output because this will include a large HTML string
        return get_final_response(base_response=base_response, output="DAG [{}] is now fresh as a daisy".format(dag_id))

    @staticmethod
    def execute_cli_command_background_mode(airflow_cmd):
        logging.info("Running command in the background")
        exit_code = os.system(airflow_cmd)
        output = REST_API.get_empty_process_output()
        output["stdout"] = "exit_code: " + str(exit_code)
        return output

    @staticmethod
    def execute_cli_command(airflow_cmd_split):
        process = subprocess.Popen(airflow_cmd_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        return REST_API.collect_process_output(process)

    @staticmethod
    def get_empty_process_output():
        return {
            "stderr": "",
            "stdin": "",
            "stdout": ""
        }

    @staticmethod
    def collect_process_output(process):
        output = REST_API.get_empty_process_output()
        if process.stderr is not None:
            output["stderr"] = ""
            for line in process.stderr.readlines():
                output["stderr"] += str(line)
        if process.stdin is not None:
            output["stdin"] = ""
            for line in process.stdin.readlines():
                output["stdin"] += str(line)
        if process.stdout is not None:
            output["stdout"] = ""
            for line in process.stdout.readlines():
                output["stdout"] += str(line)
        logging.info("RestAPI Output: " + str(output))
        return output

    @staticmethod
    def filter_loading_messages(output):
        stdout = output["stdout"]
        new_stdout_array = stdout.split("\n")
        content_to_remove_greatest_index = 0
        for index, content in enumerate(new_stdout_array):
            if content.startswith("["):
                content_to_remove_greatest_index = index
        content_to_remove_greatest_index += 1
        if len(new_stdout_array) > content_to_remove_greatest_index:
            new_stdout_array = new_stdout_array[content_to_remove_greatest_index:]
            output["stdout"] = "\n".join(new_stdout_array)
        return output

rest_api_view = REST_API(category="Admin", name="REST API Plugin")

rest_api_bp = Blueprint(
    "rest_api_bp",
    __name__,
    template_folder='templates',
    static_folder='static',
    static_url_path='/static/'
)


class REST_API_Plugin(AirflowPlugin):
    name = "rest_api"
    operators = []
    flask_blueprints = [rest_api_bp]
    hooks = []
    executors = []
    admin_views = [rest_api_view]
    menu_links = []
