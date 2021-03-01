import os
import re

import pendulum
from airflow import AirflowException, models, conf
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.utils.log.file_task_handler import FileTaskHandler


class PluginUtils:
    @staticmethod
    def get_task_logs(dag_id, task_id, pendulum_execution_date, session):
        if not dag_id or not task_id or not pendulum_execution_date:
            return None

        dagbag = models.DagBag(
            os.devnull,  # to initialize an empty dag bag
            store_serialized_dags=STORE_SERIALIZED_DAGS,
        )
        dag = dagbag.get_dag(dag_id)
        ti = (
            session.query(models.TaskInstance)
            .filter(
                models.TaskInstance.dag_id == dag_id,
                models.TaskInstance.task_id == task_id,
                models.TaskInstance.execution_date == pendulum_execution_date,
            )
            .first()
        )
        ti.task = dag.get_task(ti.task_id)

        file_task_handler = FileTaskHandler(
            base_log_folder=conf.get("core", "BASE_LOG_FOLDER"),
            filename_template=conf.get("core", "LOG_FILENAME_TEMPLATE"),
        )
        logs, metadatas = file_task_handler.read(ti, None, None)
        return logs, metadatas