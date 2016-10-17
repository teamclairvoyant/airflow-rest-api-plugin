from airflow import configuration
from airflow.plugins_manager import AirflowPlugin
from flask import request
from flask_admin import BaseView, expose
from datetime import datetime
import logging
import subprocess


WEBSERVER_BASE_URL = configuration.get('webserver', 'BASE_URL')

REST_API_BASE_URL = "/admin/restapi"  # NOTE: this will not override the actual Base URL to the endpoint. It is here just to fill out the bellow strings.
TRIGGER_DAG_URL = "/api/v1.0/trigger_dag"


class RESTAPI(BaseView):

    @expose('/')
    def index(self):
        return """
<html">
<body>

<h1>Airflow REST API</h1>

<h3>Trigger Dag</h3>

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
                <td><input type="submit" value="Trigger Dag"/></td>
            </tr>
        </table>
    </form>
</div>

</body>
</html>"""

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

        output = ""
        if process.stderr is not None:
            output += "stderr: "
            for line in process.stderr.readlines():
                output += str(line)
        if process.stdin is not None:
            output += "stdin: "
            for line in process.stdin.readlines():
                output += str(line)
        if process.stdout is not None:
            output += "stdout: "
            for line in process.stdout.readlines():
                output += str(line)
        logging.info("RestAPI Output: " + output)

        return str({"status": "OK", "dag_id": dag_id, "run_id": run_id, "conf": conf})

view = RESTAPI(category="Admin", name="Airflow REST API")


class RESTAPIPlugin(AirflowPlugin):
    name = "RESTAPI"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = [view]
    menu_links = []
