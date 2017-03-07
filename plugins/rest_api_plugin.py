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

# todo: display the output of the commands nicer
# todo: test functions

apis = [
    {
        "name": "version",
        "description": "Displays the version of Airflow you're using",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": []
    },
    {
        "name": "render",
        "description": "Render a task instance's template(s)",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "variables",
        "description": "CRUD operations on variables",
        "airflow_version": "1.7.1.0",
        "http_method": "GET",
        "parameters": [
            {"name": "set", "form_type": "text", "required": False},
            {"name": "get", "form_type": "text", "required": False},
            {"name": "json", "form_type": "checkbox", "required": False},
            {"name": "default", "form_type": "text", "required": False},
            {"name": "import", "form_type": "text", "required": False},
            {"name": "export", "form_type": "text", "required": False},
            {"name": "delete", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "connections",
        "description": "List/Add/Delete connections",
        "airflow_version": "1.7.1.0",
        "http_method": "GET",
        "parameters": [
            {"name": "list", "form_type": "checkbox", "required": False},
            {"name": "add", "form_type": "checkbox", "required": False},
            {"name": "delete", "form_type": "checkbox", "required": False},
            {"name": "conn_id", "form_type": "text", "required": False},
            {"name": "conn_uri", "form_type": "text", "required": False},
            {"name": "conn_extra", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "pause",
        "description": "Pauses a DAG",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "unpause",
        "description": "Unpauses a DAG",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "task_failed_deps",
        "description": "Returns the unmet dependencies for a task instance from the perspective of the scheduler. In other words, why a task instance doesn't get scheduled and then queued by the scheduler, and then run by an executor).",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "trigger_dag",
        "description": "Trigger a DAG run",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "run_id", "form_type": "text", "required": False},
            {"name": "conf", "form_type": "text", "required": False},
            {"name": "exec_date", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "test",
        "description": "Test a task instance. This will run a task without checking for dependencies or recording it's state in the database.",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "form_type": "checkbox", "required": False},
            {"name": "dry_run", "form_type": "text", "required": False},
            {"name": "task_params", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "dag_state",
        "description": "Get the status of a dag run",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "run",
        "description": "Run a single task instance",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "mark_success", "form_type": "checkbox", "required": False},
            {"name": "force", "form_type": "checkbox", "required": False},
            {"name": "pool", "form_type": "text", "required": False},
            {"name": "cfg_path", "form_type": "text", "required": False},
            {"name": "local", "form_type": "checkbox", "required": False},
            {"name": "ignore_all_dependencies", "form_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "form_type": "checkbox", "required": False},
            {"name": "ignore_depends_on_past", "form_type": "checkbox", "required": False},
            {"name": "ship_dag", "form_type": "checkbox", "required": False},
            {"name": "pickle", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "list_tasks",
        "description": "List the tasks within a DAG",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "tree", "form_type": "checkbox", "required": False},
            {"name": "subdir", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "backfill",
        "description": "Run subsections of a DAG for a specified date range",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "task_regex", "form_type": "text", "required": False},
            {"name": "start_date", "form_type": "text", "required": False},
            {"name": "end_date", "form_type": "text", "required": False},
            {"name": "mark_success", "form_type": "checkbox", "required": False},
            {"name": "local", "form_type": "checkbox", "required": False},
            {"name": "donot_pickle", "form_type": "checkbox", "required": False},
            {"name": "include_adhoc", "form_type": "checkbox", "required": False},
            {"name": "ignore_dependencies", "form_type": "checkbox", "required": False},
            {"name": "ignore_first_depends_on_past", "form_type": "checkbox", "required": False},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "pool", "form_type": "text", "required": False},
            {"name": "dry_run", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "list_dags",
        "description": "List all the DAGs",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "report", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "kerberos",
        "description": "Start a kerberos ticket renewer",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "principal", "form_type": "text", "required": True},
            {"name": "keytab", "form_type": "text", "required": False},
            {"name": "pid", "form_type": "text", "required": False},
            {"name": "daemon", "form_type": "checkbox", "required": False},
            {"name": "stdout", "form_type": "text", "required": False},
            {"name": "stderr", "form_type": "text", "required": False},
            {"name": "log-file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "worker",
        "description": "Start a Celery worker node",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "do_pickle", "form_type": "checkbox", "required": False},
            {"name": "queues", "form_type": "text", "required": False},
            {"name": "concurrency", "form_type": "text", "required": False},
            {"name": "pid", "form_type": "checkbox", "required": False},
            {"name": "daemon", "form_type": "checkbox", "required": False},
            {"name": "stdout", "form_type": "text", "required": False},
            {"name": "stderr", "form_type": "text", "required": False},
            {"name": "log-file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "flower",
        "description": "Start a Celery worker node",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "hostname", "form_type": "text", "required": False},
            {"name": "port", "form_type": "text", "required": False},
            {"name": "flower_conf", "form_type": "text", "required": False},
            {"name": "broker_api", "form_type": "text", "required": False},
            {"name": "pid", "form_type": "text", "required": False},
            {"name": "daemon", "form_type": "checkbox", "required": False},
            {"name": "stdout", "form_type": "text", "required": False},
            {"name": "stderr", "form_type": "text", "required": False},
            {"name": "log-file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "scheduler",
        "description": "Start a scheduler instance",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": False},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "run-duration", "form_type": "text", "required": False},
            {"name": "num_runs", "form_type": "text", "required": False},
            {"name": "do_pickle", "form_type": "text", "required": False},
            {"name": "pid", "form_type": "checkbox", "required": False},
            {"name": "daemon", "form_type": "checkbox", "required": False},
            {"name": "stdout", "form_type": "text", "required": False},
            {"name": "stderr", "form_type": "text", "required": False},
            {"name": "log-file", "form_type": "text", "required": False},
        ]
    },
    {
        "name": "task_state",
        "description": "Get the status of a task instance",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "task_id", "form_type": "text", "required": True, "cli_end_position": 2},
            {"name": "execution_date", "form_type": "text", "required": True, "cli_end_position": 3},
            {"name": "subdir", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "pool",
        "description": "CRUD operations on pools",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "set", "form_type": "checkbox", "required": False},
            {"name": "get", "form_type": "text", "required": False},
            {"name": "delete", "form_type": "text", "required": False}
        ]
    },
    {
        "name": "serve_logs",
        "description": "Serve logs generate by worker",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": []
    },
    {
        "name": "clear",
        "description": "Clear a set of task instance, as if they never ran",
        "airflow_version": "1.7.0",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True, "cli_end_position": 1},
            {"name": "subdir", "form_type": "text", "required": False},
            {"name": "task_regex", "form_type": "text", "required": False},
            {"name": "start_date", "form_type": "text", "required": False},
            {"name": "end_date", "form_type": "text", "required": False},
            {"name": "upstream", "form_type": "checkbox", "required": False},
            {"name": "downstream", "form_type": "checkbox", "required": False},
            {"name": "no_confirm", "form_type": "checkbox", "required": False},
            {"name": "only_failed", "form_type": "checkbox", "required": False},
            {"name": "only_running", "form_type": "checkbox", "required": False},
            {"name": "exclude_subdags", "form_type": "checkbox", "required": False}
        ]
    },
    {
        "name": "deploy_dag",
        "description": "Deploy a new DAG File to the DAGs directory",
        "airflow_version": "None - Custom API",
        "http_method": "POST",
        "post_body_description": "dag_file - POST Body Element - REQUIRED",
        "form_enctype": "multipart/form-data",
        "parameters": [
            {"name": "dag_file", "form_type": "file", "required": True}
        ]
    },
    {
        "name": "refresh_dag",
        "description": "Refresh a DAG in the Web Server",
        "airflow_version": "None - Custom API",
        "http_method": "GET",
        "parameters": [
            {"name": "dag_id", "form_type": "text", "required": True}
        ]
    }
]


rest_api_endpoint = "/admin/rest_api/api"
airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
dags_folder = configuration.get('core', 'DAGS_FOLDER')
rest_api_plugin_http_token_header_name = "rest_api_plugin_http_token"
expected_http_token = None
if configuration.has_option("webserver", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN"):
    expected_http_token = configuration.get("webserver", "REST_API_PLUGIN_EXPECTED_HTTP_TOKEN")


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
        if expected_http_token:
            logging.info("Performing Token Authentication")
            if request.headers.get(rest_api_plugin_http_token_header_name, None) != expected_http_token:
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

        final_response = ""
        if api == "version":
            final_response = self.version(base_response)
        elif api == "deploy_dag":
            final_response = self.deploy_dag(base_response)
        elif api == "refresh_dag":
            final_response = self.refresh_dag(base_response)
        else:
            for api_object in apis:
                if api_object["name"] == api:
                    final_response = self.execute_cli(base_response, api_object)

        return final_response

    def execute_cli(self, base_response, api_object):
        logging.info("Executing cli function")

        largest_end_argument_value = 0
        for parameter in api_object["parameters"]:
            if parameter.get("cli_end_position") is not None and parameter["cli_end_position"] > largest_end_argument_value:
                largest_end_argument_value = parameter["cli_end_position"]

        command_split = ["airflow", api_object["name"]]
        end_arguments = [0] * largest_end_argument_value
        for parameter in api_object["parameters"]:
            parameter_name = parameter["name"]
            parameter_value = request.args.get(parameter_name)
            if parameter["required"]:
                if is_arg_not_provided(parameter_value):
                    return get_400_error_response(base_response, str(parameter_name) + " should be provided")
            if parameter_value:
                logging.info("parameter['cli_end_position']: " + str(parameter['cli_end_position']))
                if parameter["cli_end_position"] is not None:
                    end_arguments[parameter["cli_end_position"]-1] = parameter_value
                else:
                    command_split.extend(["--" + parameter_name, parameter_value])

        command_split.extend(end_arguments)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return get_final_response(base_response=base_response, output=output, airflow_cmd=" ".join(command_split))

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
            dag_file.save(os.path.join(dags_folder, dag_file.filename))
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

        response = urllib2.urlopen(airflow_webserver_base_url + '/admin/airflow/refresh?dag_id=' + dag_id)
        html = response.read()  # avoid using this as the output because this will include a large HTML string
        return get_final_response(base_response=base_response, output="DAG [{}] is now fresh as a daisy".format(dag_id))

    @staticmethod
    def collect_process_output(process):
        output = {}
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
