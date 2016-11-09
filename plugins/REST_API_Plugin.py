from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint, request, jsonify
from flask_admin import BaseView, expose

from datetime import datetime
import logging
import subprocess

"""
CLIs this REST API exposes are Defined here: http://airflow.incubator.apache.org/cli.html

"""

# todo: add validation to request params
# todo: list available dags on admin page

# NOTE: this will not override the actual Base URL to the endpoint. It is
# here just to fill out the bellow strings.

url_dict = dict(
    REST_API_BASE_URL="/admin/rest_api",         # Check with current version
    VERSION_URL="/api/v1.0/version",            # Check with current version
    PAUSE_URL="/api/v1.0/pause",                # Check with current version
    UNPAUSE_URL="/api/v1.0/unpause",            # Check with current version
    RUN_URL="/api/v1.0/run",                    # todo: add api
    LIST_TASKS_URL="/api/v1.0/list_tasks",      # Check with current version
    BACKFILL_URL="/api/v1.0/backfill",          # Done / Untested
    LIST_DAGS_URL="/api/v1.0/list_dags",        # Working
    TASK_STATE_URL="/api/v1.0/task_state",      # todo: add api
    TRIGGER_DAG_URL="/api/v1.0/trigger_dag",    # Check with current version
    REFRESH_DAG_URL="/api/v1.0/refresh_dag",    # todo: add api - Call: http://localhost:5555/admin/airflow/refresh?dag_id=test_hadoop_operators
    DEPLOY_DAG_URL="/api/v1.0/deploy_dag"       # todo: add api
)


class REST_API(BaseView):

    @expose('/')
    def index(self):
        return self.render("rest_api_plugin/index.html", url_dict=url_dict)

    @expose(url_dict.get("VERSION_URL"))
    def version(self):
        call_time = datetime.now()
        command_split = ["airflow", "version"]

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "output": output, "call_time": call_time, "response_time": datetime.now()})

    @expose(url_dict.get("PAUSE_URL"))
    def pause(self):
        call_time = datetime.now()
        dag_id = request.args.get('dag_id')
        sd = request.args.get('sd')

        command_split = ["airflow", "pause"]
        if sd:
            command_split.append("-sd")
            command_split.append(sd)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "sd": sd, "output": output, "call_time": call_time, "response_time": datetime.now()})

    @expose(url_dict.get("UNPAUSE_URL"))
    def unpause(self):
        call_time = datetime.now()
        dag_id = request.args.get('dag_id')
        sd = request.args.get('sd')

        command_split = ["airflow", "unpause"]
        if sd:
            command_split.append("-sd")
            command_split.append(sd)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "sd": sd, "output": output, "call_time": call_time, "response_time": datetime.now()})

    @expose(url_dict.get("LIST_TASKS_URL"))
    def listtasks(self):
        call_time = datetime.now()
        dag_id = request.args.get('dag_id')
        sd = request.args.get('sd')
        tree = request.args.get('tree')
        command_split = ["airflow", "list_tasks"]
        if sd:
            command_split.append("-sd")
            command_split.append(sd)
        if tree:
            command_split.append("-t")
            command_split.append(tree)
        command_split.append(dag_id)
        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "output": output, "call_time": call_time, "response_time": datetime.now()})

    @expose(url_dict.get("TRIGGER_DAG_URL"))
    def trigger_dag(self):
        call_time = datetime.now()
        dag_id = request.args.get('dag_id')
        execution_date = call_time.isoformat()
        run_id = request.args.get(
            'run_id') or "restapi_trig__" + execution_date
        conf = request.args.get('conf')
        command_split = ["airflow", "trigger_dag"]
        if run_id is not None:
            command_split.append("--run_id")
            command_split.append(run_id)
        if conf is not None:
            command_split.append("--conf")
            command_split.append(conf)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(
            command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "run_id": run_id, "conf": conf, "output": output, "call_time": call_time, "response_time": datetime.now()})

    # todo: finished this
    # @expose(url_dict.get("BACKFILL_URL"))
    # def trigger_dag(self):
    #     call_time = datetime.now()
    #     dag_id = request.args.get('dag_id')
    #     execution_date = call_time.isoformat()
    #     command_split = ["airflow", "backfill"]
    #     if task_regex is not None:
    #          command_split.append("--task_regex")
    #          command_split.append(task_regex)
    #     if start_date is not None:
    #          command_split.append("--start_date")
    #          command_split.append(start_date)
    #     if end_date is not None:
    #          command_split.append("--end_date")
    #          command_split.append(end_date)
    #     if mark_success is not None:
    #          command_split.append("--mark_success")
    #          command_split.append(mark_success)
    #     if local is not None:
    #          command_split.append("--local")
    #          command_split.append(local)
    #     if donot_pickle is not None:
    #          command_split.append("--donot_pickle")
    #          command_split.append(donot_pickle)
    #     if include_adhoc is not None:
    #          command_split.append("--include_adhoc")
    #          command_split.append(include_adhoc)
    #     if ignore_dependencies is not None:
    #          command_split.append("--ignore_dependencies")
    #          command_split.append(ignore_dependencies)
    #     if ignore_first_depends_on_past is not None:
    #          command_split.append("--ignore_first_depends_on_past")
    #          command_split.append(ignore_first_depends_on_past)
    #     if subdir is not None:
    #          command_split.append("--subdir")
    #          command_split.append(subdir)
    #     if pool is not None:
    #          command_split.append("--pool")
    #          command_split.append(pool)
    #     if dry_run is not None:
    #          command_split.append("--dry_run")
    #          command_split.append(dry_run)
    #     command_split.append(dag_id)
    #
    #     logging.info("command_split array: " + str(command_split))
    #     process = subprocess.Popen(
    #         command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     process.wait()
    #
    #     output = self.collect_process_output(process)
    #
    #     return jsonify({"status": "OK", "dag_id": dag_id, "output": output, "call_time": call_time, "response_time": datetime.now()})

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
