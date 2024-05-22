import json
import os
import sys

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


basedir = os.path.abspath(os.path.dirname(__file__))

operator_mapping = {
    "PythonOperator": PythonOperator,
    "BashOperator": BashOperator,
    "DummyOperator": DummyOperator,
    "TriggerDagRunOperator": TriggerDagRunOperator,
    "BranchPythonOperator": BranchPythonOperator,
}

class BasicAirflowPipeline:
    pass