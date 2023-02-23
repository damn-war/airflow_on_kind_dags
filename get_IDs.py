from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import hashlib
import logging

logger = logging.getLogger("airflow.task")

logger.setLevel(logging.CRITICAL)

args = {"owner": "Daniel", "start_date": days_ago(1)}

dag = DAG(dag_id="GET_THE_IDs", default_args=args, schedule_interval=None)

# get an numeric hash value from the dagrun_id
def get_numeric_hash(run_id, hashlib_hashfunc):
    # pass an dag run_id and a hashlib hashfunction
    # get the hash value
    hashvalue = hashlib_hashfunc(run_id.encode("utf-8")).hexdigest()
    # transform to integer
    inthash = int(hashvalue, 16)
    # if needed: only get the last X digits, here 16
    # inthash = inthash % 10**16
    return inthash


def python_get_ids(run_id, task_instance, **context):
    numerical_run_id = get_numeric_hash(run_id, hashlib.sha256)
    print("Getting the IDs with the PythonOperator")
    print(f"DagRun_ID:        {run_id}")
    print(f"DagRun_ID hashed: {numerical_run_id}")
    print(f"Job_ID:           {task_instance.job_id}")
    logger.info("Test")
    task_instance.xcom_push(key="numerical_run_id", value=numerical_run_id)


def test_xcom_passing(task_instance):
    numerical_run_id = task_instance.xcom_pull(
        key="numerical_run_id", task_ids="Python_get_IDs"
    )
    print(f"DagRun_ID hashed: {numerical_run_id}")


with dag:
    python_get_ids = PythonOperator(task_id="Task", python_callable=python_get_ids)

    bash_get_ids = BashOperator(
        task_id="Bash_get_IDs",
        bash_command="echo DagRun_ID: {{ run_id }};"
        "hexnum=($(echo -n {{ run_id }} | sha256sum ));"
        "echo DagRun_ID hashed: $(python -c \"print(int('${hexnum}', 16))\";)\n"
        "echo Job_ID: {{ task_instance.job_id }}",
    )

    python_test_xcom_passing = PythonOperator(
        task_id="Python_test_XCOM_passing", python_callable=test_xcom_passing
    )

    python_get_ids >> bash_get_ids >> python_test_xcom_passing
