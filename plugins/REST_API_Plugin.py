from airflow import configuration
from airflow.plugins_manager import AirflowPlugin
from flask import request, jsonify
from flask_admin import BaseView, expose
from datetime import datetime

import logging
import subprocess

"""

CLIs this REST API exposes are Defined here: http://airflow.incubator.apache.org/cli.html

"""

# todo: add validation to request params

REST_API_BASE_URL = "/admin/restapi"  # NOTE: this will not override the actual Base URL to the endpoint. It is here just to fill out the bellow strings.
VERSION_URL = "/api/v1.0/version"
PAUSE_URL = "/api/v1.0/pause"
UNPAUSE_URL = "/api/v1.0/unpause"
DAG_STATE_URL = "/api/v1.0/dag_state"
RUN_URL = "/api/v1.0/run"
LIST_TASKS_URL = "/api/v1.0/list_tasks"
BACKFILL_URL = "/api/v1.0/backfill"
LIST_DAGS_URL = "/api/v1.0/list_dags"
TASK_STATE_URL = "/api/v1.0/task_state"
TRIGGER_DAG_URL = "/api/v1.0/trigger_dag"


class RESTAPI(BaseView):

    @expose('/')
    def index(self):
        return """
<html>
<body>

    <h1>Airflow REST API</h1>

    <div>
        <h3>Version</h3>
        <h5>TODO: Create documentation</h5>
        <h5>http://{HOSTNAME}:{PORT}""" + REST_API_BASE_URL + VERSION_URL + """</h5>
        <div>
            <form method="GET" action=" """ + REST_API_BASE_URL + VERSION_URL + """ ">
                <table>
                    <tr>
                        <td></td>
                        <td><input type="submit" value="Get Version"/></td>
                    </tr>
                </table>
            </form>
        </div>
    </div>

    <div>
        <h3>Pause DAG</h3>
        <h5>TODO: Create documentation</h5>
        <h5>http://{HOSTNAME}:{PORT}""" + REST_API_BASE_URL + PAUSE_URL + """?dag_id={DAG_ID-REQUIRED}&sb={SUB_DIRECTORY-OPTIONAL}</h5>
        <div>
            <form method="GET" action=" """ + REST_API_BASE_URL + PAUSE_URL + """ ">
                <table>
                    <tr>
                        <td>DAG ID:</td>
                        <td><input type="text" name="dag_id"/></td>
                    </tr>
                    <tr>
                        <td>SubDirectory (Optional):</td>
                        <td><input type="text" name="sd"/></td>
                    </tr>
                    <tr>
                        <td></td>
                        <td><input type="submit" value="Pause DAG"/></td>
                    </tr>
                </table>
            </form>
        </div>
    </div>

    <div>
        <h3>Unpause DAG</h3>
        <h5>TODO: Create documentation</h5>
        <h5>http://{HOSTNAME}:{PORT}""" + REST_API_BASE_URL + UNPAUSE_URL + """?dag_id={DAG_ID-REQUIRED}&sb={SUB_DIRECTORY-OPTIONAL}</h5>
        <div>
            <form method="GET" action=" """ + REST_API_BASE_URL + UNPAUSE_URL + """ ">
                <table>
                    <tr>
                        <td>DAG ID:</td>
                        <td><input type="text" name="dag_id"/></td>
                    </tr>
                    <tr>
                        <td>SubDirectory (Optional):</td>
                        <td><input type="text" name="sd"/></td>
                    </tr>
                    <tr>
                        <td></td>
                        <td><input type="submit" value="Unpause DAG"/></td>
                    </tr>
                </table>
            </form>
        </div>
    </div>

    <div>
        <h3>Trigger DAG</h3>
        <h5>TODO: Create documentation</h5>
        <h5>http://{HOSTNAME}:{PORT}""" + REST_API_BASE_URL + TRIGGER_DAG_URL + """?dag_id={DAG_ID-REQUIRED}&run_id={RUN_ID-OPTIONAL}&conf={CONF_JSON-OPTIONAL}</h5>
        <div>
            <form method="GET" action=" """ + REST_API_BASE_URL + TRIGGER_DAG_URL + """ ">
                <table>
                    <tr>
                        <td>DAG ID:</td>
                        <td><input type="text" name="dag_id"/></td>
                    </tr>
                    <tr>
                        <td>Run ID (Optional):</td>
                        <td><input type="text" name="run_id"/></td>
                    </tr>
                    <tr>
                        <td>Conf Json (Optional):</td>
                        <td><input type="text" name="conf"/></td>
                    </tr>
                    <tr>
                        <td></td>
                        <td><input type="submit" value="Trigger DAG"/></td>
                    </tr>
                </table>
            </form>
        </div>
    </div>

</body>
</html>"""

    @expose(VERSION_URL)
    def version(self):
        command_split = ["airflow", "version"]

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "output": output})

    @expose(PAUSE_URL)
    def pause(self):
        dag_id = request.args.get('dag_id')
        sd = request.args.get('sd')

        command_split = ["airflow", "pause"]
        if sd:
            command_split.append("-sd")
            command_split.append(sd)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "sd": sd, "output": output})

    @expose(UNPAUSE_URL)
    def unpause(self):
        dag_id = request.args.get('dag_id')
        sd = request.args.get('sd')

        command_split = ["airflow", "unpause"]
        if sd:
            command_split.append("-sd")
            command_split.append(sd)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "sd": sd, "output": output})

    @expose(TRIGGER_DAG_URL)
    def trigger_dag(self):
        dag_id = request.args.get('dag_id')
        execution_date = datetime.now().isoformat()
        run_id = request.args.get('run_id') or "restapi_trig__" + execution_date
        conf = request.args.get('conf')

        command_split = ["airflow", "trigger_dag", "--run_id", run_id]
        if conf is not None:
            command_split.append("--conf")
            command_split.append(conf)
        command_split.append(dag_id)

        logging.info("command_split array: " + str(command_split))
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        output = self.collect_process_output(process)

        return jsonify({"status": "OK", "dag_id": dag_id, "run_id": run_id, "conf": conf, "output": output})

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

view = RESTAPI(category="Admin", name="Airflow REST API")


class RESTAPIPlugin(AirflowPlugin):
    name = "RESTAPI"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = [view]
    menu_links = []
