import importlib
from datetime import datetime

from airflow import DAG
from dags_config.dags_configuration import operator_mapping


class DAGGenerator:
    def __init__(
        self,
        dag_id: str = "",
        dag_config: object = None,
        dag_args: dict = {
            "owner": "Trieulv",
            "start_date": datetime(2023, 9, 1),
            "depends_on_past": False,
        },
    ) -> None:
        self.dag_id = dag_id
        self.dag_config = dag_config
        self.dag_args = dag_args

    def create_dag(self, schedule_interval: str = None, catchup=False, **kwargs):
        """Create a DAG from the configuration

        Args:
            schedule_interval (str, optional): Set schedule for DAGs. Example: `@daily`,`@monthly`, `@hourly` or use `****. Defaults to None.
            catchup (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        with DAG(
            dag_id=self.dag_id,
            schedule_interval=schedule_interval,
            default_args=self.dag_args,
            catchup=catchup,
            **kwargs,
        ) as dag:
            tasks_pipeline = {}
            for task_info in self.dag_config.tasks_list:
                if task_info["should_run"]:
                    operator = operator_mapping[task_info["operator"]]
                    if task_info["operator"] in ["PythonOperator", "BranchPythonOperator"]:
                        python_callable = getattr(
                            importlib.import_module("tasks"), task_info["python_callable"]
                        )
                        kwargs = task_info["kwargs"]
                        task = operator(
                            task_id=task_info["task_id"],
                            python_callable=python_callable,
                            op_kwargs=kwargs,
                            provide_context=True,
                            trigger_rule=task_info.get("trigger_rule", "all_success"),
                        )
                    elif task_info["operator"] == "TriggerDagRunOperator":
                        task = operator(
                            task_id=task_info["task_id"],
                            **task_info["kwargs"],
                            trigger_rule=task_info.get("trigger_rule", "all_success"),
                        )
                    else:
                        task = operator(
                            task_id=task_info["task_id"],
                            provide_context=True,
                            trigger_rule=task_info.get("trigger_rule", "all_success"),
                        )

                    tasks_pipeline[task_info["task_id"]] = task

                    # Set dependency
                    upstream_tasks = task_info["upstream_tasks"]
                    if upstream_tasks:
                        upstream_task_objs = [tasks_pipeline[t] for t in upstream_tasks]
                        task.set_upstream(upstream_task_objs)
        return dag
