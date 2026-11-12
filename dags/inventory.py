"""Capture source stock near midnight; never backfill an EOD from today's current stock."""

from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.timetables.trigger import CronTriggerTimetable

from shopvn.orchestration.inventory import begin, capture, finalize, verify

KEY = "{{ ti.xcom_pull(task_ids='initialize') }}"
SNAPSHOT = "{{ ti.xcom_pull(task_ids='capture_source') }}"
MODE = "{{ ti.xcom_pull(task_ids='capture_source', key='mode') }}"
DAY = "{{ (ti.xcom_pull(task_ids='capture_source', key='business_date') or '') }}"

with DAG(
    dag_id="shopvn_inventory_observation",
    description="Actual stock observation; scheduled EOD capture within 60 seconds after midnight VN",
    schedule=CronTriggerTimetable("0 0 * * *", timezone="Asia/Ho_Chi_Minh", run_immediately=False),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "shopvn-data-team",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        "execution_timeout": timedelta(minutes=10),
    },
    params={
        "mode": Param("current", enum=["current", "eod"]),
        "business_date": Param("", type="string"),
    },
    tags=["shopvn", "inventory", "actual", "cutoff"],
) as dag:
    initialize = PythonOperator(task_id="initialize", python_callable=begin)
    extract = PythonOperator(task_id="capture_source", python_callable=capture, priority_weight=100)
    transform = SparkSubmitOperator(
        task_id="load_observation",
        conn_id="spark_local",
        pool="spark_local",
        durable=False,
        application="/workspace/scripts/inventory_spark.py",
        driver_memory="1g",
        application_args=[
            "--snapshot-id",
            SNAPSHOT,
            "--mode",
            MODE,
            "--business-date",
            DAY,
            "--result-path",
            "/workspace/runtime/orchestration/runs/" + KEY + "/inventory-result.json",
        ],
        conf={"spark.ui.enabled": "false"},
    )
    confirm = PythonOperator(task_id="verify_inventory", python_callable=verify)
    finish = PythonOperator(
        task_id="finalize", python_callable=finalize, trigger_rule="all_done", retries=0
    )
    initialize >> extract >> transform >> confirm
    [initialize, extract, transform, confirm] >> finish
