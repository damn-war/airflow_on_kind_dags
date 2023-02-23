from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import logging

logger_DAG = logging.getLogger("airflow.task")

logger_DAG.setLevel(logging.CRITICAL)

args = {"owner": "Daniel", "start_date": days_ago(1)}

dag = DAG(
    dag_id="A_Logging_Test_DAG",  # <- 'A_' only to have the DAG on first place in the UI
    default_args=args,
    schedule_interval=None,
)


def loggingtask():
    logger_DAG.debug("    DEBUG    for DAG logger")
    logger_DAG.info("     INFO     for DAG logger")
    logger_DAG.warning("  WARNING  for DAG logger")
    logger_DAG.error("    ERROR    for DAG logger")
    logger_DAG.critical(" CRITICAL for DAG logger")

    logger_TASK = logging.getLogger("airflow.task")
    logger_TASK.setLevel(logging.CRITICAL)
    logger_TASK.debug("    DEBUG    for TASK logger")
    logger_TASK.info("     INFO     for TASK logger")
    logger_TASK.warning("  WARNING  for TASK logger")
    logger_TASK.error("    ERROR    for TASK logger")
    logger_TASK.critical(" CRITICAL for TASK logger")


with dag:
    loggingtask = PythonOperator(
        task_id="LoggingTestTask",
        python_callable=loggingtask,
    )

    loggingtask
