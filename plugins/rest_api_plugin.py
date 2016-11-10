from airflow.models import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow import configuration

from flask import Blueprint, request, jsonify
from flask_admin import BaseView, expose

from datetime import datetime
import airflow
import logging
import subprocess

"""
CLIs this REST API exposes are Defined here: http://airflow.incubator.apache.org/cli.html
"""

# todo: add validation to request params
# todo: display the output of the commands nicer

url_dict = dict(
    REST_API_BASE_URL="/admin/rest_api",
    VERSION_URL="/api/v1.0/version",
    VARIABLES_URL="/api/v1.0/variables",
    PAUSE_URL="/api/v1.0/pause",
    UNPAUSE_URL="/api/v1.0/unpause",
    TEST_URL="/api/v1.0/test",
    DAG_STATE_URL="/api/v1.0/dag_state",
    RUN_URL="/api/v1.0/run",
    LIST_TASKS_URL="/api/v1.0/list_tasks",
    BACKFILL_URL="/api/v1.0/backfill",
    LIST_DAGS_URL="/api/v1.0/list_dags",
    KERBEROS_URL="/api/v1.0/kerberos",
    WORKER_URL="/api/v1.0/worker",
    SCHEDULER_URL="/api/v1.0/scheduler",
    TASK_STATE_URL="/api/v1.0/task_state",
    TRIGGER_DAG_URL="/api/v1.0/trigger_dag",
    REFRESH_DAG_URL="/api/v1.0/refresh_dag",
    DEPLOY_DAG_URL="/api/v1.0/deploy_dag"
)


class REST_API(BaseView):

    @expose('/')
    def index(self):
        dagbag = DagBag()
        # todo: get hostname and
        airflow_webserver_base_url = configuration.get('webserver', 'BASE_URL')
        return self.render("rest_api_plugin/index.html", dags=dagbag.dags, airflow_webserver_base_url=airflow_webserver_base_url, url_dict=url_dict)

    @expose(url_dict.get("VERSION_URL"))
    def version(self):
        base_response = self.get_base_response()
        return self.get_final_response(base_response, airflow.__version__)

    # todo: implement
    @expose(url_dict.get("VARIABLES_URL"))
    def variables(self):
        raise NotImplementedError

    @expose(url_dict.get("PAUSE_URL"))
    def pause(self):
        base_response = self.get_base_response()
        dag_id = request.args.get('dag_id')
        subdir = request.args.get('subdir')

        if dag_id is None:
            raise ValueError("dag_id should be provided")

        command_split = ["airflow", "pause"]
        if subdir:
            command_split.extend(["--subdir", subdir])
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return self.get_final_response(base_response, output)

    @expose(url_dict.get("UNPAUSE_URL"))
    def unpause(self):
        base_response = self.get_base_response()
        dag_id = request.args.get('dag_id')
        subdir = request.args.get('subdir')

        if dag_id is None:
            raise ValueError("dag_id should be provided")

        command_split = ["airflow", "unpause"]
        if subdir:
            command_split.extend(["--subdir", subdir])
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return self.get_final_response(base_response, output)

    # todo: implement
    @expose(url_dict.get("TEST_URL"))
    def test(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("DAG_STATE_URL"))
    def dag_state(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("RUN_URL"))
    def run(self):
        raise NotImplementedError

    @expose(url_dict.get("LIST_TASKS_URL"))
    def list_tasks(self):
        base_response = self.get_base_response()
        dag_id = request.args.get('dag_id')
        subdir = request.args.get('subdir')

        if dag_id is None:
            raise ValueError("dag_id should be provided")

        command_split = ["airflow", "list_tasks"]
        if subdir:
            command_split.extend(["--subdir", subdir])
        if request.args.get('tree') is not None:
            command_split.append("--tree")
        command_split.append(dag_id)
        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return self.get_final_response(base_response, output)

    # todo: finished this
    @expose(url_dict.get("BACKFILL_URL"))
    def trigger_dag(self):
        base_response = self.get_base_response()
        dag_id = request.args.get('dag_id')
        task_regex = request.args.get('task_regex')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        subdir = request.args.get('subdir')
        pool = request.args.get('pool')

        if dag_id is None:
            raise ValueError("dag_id should be provided")

        command_split = ["airflow", "backfill"]
        if task_regex is not None:
            command_split.extend(["--task_regex", task_regex])
        if start_date is not None:
            command_split.extend(["--start_date", start_date])
        if end_date is not None:
            command_split.extend(["--end_date", end_date])
        if request.args.get('mark_success') is not None:
            command_split.append("--mark_success")
        if request.args.get('local') is not None:
            command_split.append("--local")
        if request.args.get('donot_pickle') is not None:
            command_split.append("--donot_pickle")
        if request.args.get('include_adhoc') is not None:
            command_split.append("--include_adhoc")
        if request.args.get('ignore_dependencies') is not None:
            command_split.append("--ignore_dependencies")
        if request.args.get('ignore_first_depends_on_past') is not None:
            command_split.append("--ignore_first_depends_on_past")
        if subdir is not None:
            command_split.extend(["--subdir", subdir])
        if pool is not None:
            command_split.extend(["--pool", pool])
        if request.args.get('dry_run') is not None:
            command_split.append("--dry_run")
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return self.get_final_response(base_response, output)

    # todo: implement
    @expose(url_dict.get("LIST_DAGS_URL"))
    def list_dags(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("KERBEROS_URL"))
    def kerberos(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("WORKER_URL"))
    def worker(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("SCHEDULER_URL"))
    def scheduler(self):
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("TASK_STATE_URL"))
    def task_state(self):
        raise NotImplementedError

    @expose(url_dict.get("TRIGGER_DAG_URL"))
    def trigger_dag(self):
        call_time = datetime.now()
        execution_date = call_time.isoformat()
        base_response = self.get_base_response(call_time)
        dag_id = request.args.get('dag_id')
        run_id = request.args.get('run_id') or "restapi_trig__" + execution_date
        conf = request.args.get('conf')

        if dag_id is None:
            raise ValueError("dag_id should be provided")

        command_split = ["airflow", "trigger_dag"]
        if run_id is not None:
            command_split.extend(["--run_id", run_id])
        if conf is not None:
            command_split.extend(["--conf", conf])
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)
        base_response["run_id"] = run_id

        return self.get_final_response(base_response, output)

    # todo: implement
    @expose(url_dict.get("REFRESH_DAG_URL"))
    def refresh_dag(self):
        # Call: http://localhost:5555/admin/airflow/refresh?dag_id=test_hadoop_operators
        raise NotImplementedError

    # todo: implement
    @expose(url_dict.get("DEPLOY_DAG_URL"))
    def deploy_dag(self):
        raise NotImplementedError

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

    @staticmethod
    def get_base_response(call_time=datetime.now()):
        return {"status": "OK", "arguments": request.args, "call_time": call_time}

    @staticmethod
    def get_final_response(base_response, output):
        final_response = base_response
        final_response["response_time"] = datetime.now()
        final_response["output"] = output
        return jsonify(final_response)


rest_api_view = REST_API(category="Admin", name="REST API")

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
