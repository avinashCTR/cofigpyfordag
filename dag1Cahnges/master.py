"""
This DAG triggers the search_engine_legos_jiomart_complete_catalog_standardised DAG.
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import BaseOperator
from search_engine_legos_jiomart_complete_catalog_standardized import task_group

from jiomart_legos_conf import task_configs as conf  # Importing custom config

date = datetime(2023, 8, 1)

default_args = {
    "owner": "couture",
    "depends_on_past": False,
    # "config_group": "config_group_old_cluster",
    "config_group": "config_group_jiomart_large",
    "start_date": date,
    "retries": 0,  # Temporarily kept because of "No Host supplied" error in WFO
    "retry_delay": timedelta(minutes=1)
}

paths_conf = {
  "dirBasePath":"/data1/searchengine/",
  "catalogue_label": "jiomart",
  "catalogue_date" : "05092024",
  "catalogue_date_old":"05092024",
  "version_new":"",
  "version_old":"V1_delta_changes",
  "testing":"false"
}

Dag = DAG("search_engine_legos_jiomart_master", default_args=default_args, concurrency=4, schedule_interval=None, tags=["search-engine"])

TARGET_DAG_ID = "search_engine_legos_jiomart_complete_catalog_standardized"

verticals = [
    "Fashion",
    "Electronics",
    "Home_Lifestyle",
    "Groceries",
    "Industrial_Professional_Supplies",
    "Books_Music_Stationery",
    "Furniture",
    "Beauty",
    "Sports_Toys_Luggage",
    "Wellness",
    "Crafts_of_India",
    "Precious_Jewellery",
    "Premium_Fruits",
]

with Dag:

    # Dummy task to trigger the DAG group
    alltriggerTask = DummyOperator(task_id="TriggerTask")

    tasks =[]

    for vertical in verticals:
        
        triggerTask = DummyOperator(task_id=vertical + "TriggerTask")
            
        vertical_task_dag = task_group(Dag,vertical,conf,paths_conf) 
        tasks.append(vertical_task_dag)

        alltriggerTask >> triggerTask >> tasks
