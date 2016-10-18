# Airflow REST API Plugin

### Description

A plugin for Apache Airflow (https://github.com/apache/incubator-airflow) that exposes rest end points for the Command Line Interfaces listed in the airflow documentation:

http://airflow.incubator.apache.org/cli.html

### Deployment Instructions

1. Create the plugins folder if it doesn't exist. 

    * The location you should put it is usually at {AIRFLOW_HOME}/plugins. The specific location can be found in your airflow.cfg file:
    
        ```
              
            plugins_folder = /home/{USER_NAME}/airflow/plugins
              
        ```
    
2. Copy the contents of the plugins folder from this repo into the plugins folder you created on the Airflow server.

3. Restart the Airflow Web Server

### Using the REST API

Once you deploy the plugin and restart the web server, you can start to use the REST API. Bellow you will see the endpoints that are supported. In addition, you can also interact with the REST API from the Airflow Web Server. When you reload the page, you will see a link under the Admin tab called "Airflow REST API". Clicking on this will take you to the following URL:

http://{HOST}:{PORT}/admin/restapi/

This web page will show the Endpoints supported and provide a form for you to test submitting to them.
 
#### Endpoints


###### Trigger DAG

GET - http://{HOST}:{PORT}/admin/restapi/api/v1.0/trigger_dag

Query Arguments:
    
* dag_id - The DAG ID of the DAG you want to trigger
     
* run_id (Optional) - The RUN ID to use for the DAG run

* conf - Some configuration to pass to the DAG you trigger

Examples:

http://{HOST}:{PORT}/admin/restapi/api/v1.0/trigger_dag?dag_id=test_id

http://{HOST}:{PORT}/admin/restapi/api/v1.0/trigger_dag?dag_id=test_id&run_id=run_id_2016_01_01&conf=%7B%22key%22%3A%22value%22%7D

