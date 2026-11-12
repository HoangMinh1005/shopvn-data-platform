"""S2: verified marketplace files, partial coverage and exact Finance publication gate."""

from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.timetables.trigger import CronTriggerTimetable

from shopvn.orchestration.marketplace import begin, capture, check, finalize, verify

KEY = "{{ ti.xcom_pull(task_ids='initialize') }}"
with DAG(
    dag_id="shopvn_marketplace_finance",
    schedule=CronTriggerTimetable("0 4 * * *", timezone="Asia/Ho_Chi_Minh", run_immediately=False),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "shopvn-data-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
        "execution_timeout": timedelta(minutes=15),
    },
    params={"start_date": Param("", type="string"), "end_date": Param("", type="string")},
    tags=["shopvn", "stage2", "marketplace", "finance"],
) as dag:
    initialize = PythonOperator(task_id="initialize", python_callable=begin)
    extract = PythonOperator(task_id="capture_source", python_callable=capture)
    prepare = SparkSubmitOperator(
        task_id="prepare_candidate",
        conn_id="spark_local",
        pool="spark_local",
        durable=False,
        application="/workspace/scripts/marketplace_spark.py",
        driver_memory="1g",
        application_args=[
            "prepare",
            "--capture-id",
            "{{ ti.xcom_pull(task_ids='capture_source') }}",
            "--run-key",
            KEY,
        ],
        conf={"spark.ui.enabled": "false"},
    )
    validate = PythonOperator(task_id="validate_candidate", python_callable=check)
    publish = SparkSubmitOperator(
        task_id="publish_finance",
        conn_id="spark_local",
        pool="spark_local",
        durable=False,
        application="/workspace/scripts/marketplace_spark.py",
        driver_memory="1g",
        application_args=["publish", "--run-key", KEY],
        conf={"spark.ui.enabled": "false"},
    )
    confirm = PythonOperator(task_id="verify_and_finish", python_callable=verify)
    finish = PythonOperator(
        task_id="finalize", python_callable=finalize, trigger_rule="all_done", retries=0
    )
    initialize >> extract >> prepare >> validate >> publish >> confirm
    [initialize, extract, prepare, validate, publish, confirm] >> finish
