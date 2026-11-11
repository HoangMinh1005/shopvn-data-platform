"""Daily direct Finance: immutable extract, candidate, DQ gate, atomic publication, audit."""

from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, Param, SyncCallback
from airflow.timetables.trigger import CronTriggerTimetable

from shopvn.orchestration.callbacks import deadline_exceeded, task_failed, task_retry
from shopvn.orchestration.tasks import (
    extract_snapshot,
    finalize,
    initialize,
    validate_candidate,
    verify_and_finish,
)

KEY = "{{ ti.xcom_pull(task_ids='initialize') }}"
SNAPSHOT = "{{ ti.xcom_pull(task_ids='extract_snapshot') }}"
RESULT = "/workspace/runtime/orchestration/runs/" + KEY + "/transform.json"

with DAG(
    dag_id="shopvn_direct_finance",
    description="Direct FIN02; source/Spark/Trino reconciliation before certified publication",
    schedule=CronTriggerTimetable("0 3 * * *", timezone="Asia/Ho_Chi_Minh", run_immediately=False),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=False,
    default_args={
        "owner": "shopvn-data-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=2),
        "on_failure_callback": task_failed,
        "on_retry_callback": task_retry,
        "execution_timeout": timedelta(minutes=15),
    },
    params={
        "business_date": Param(
            "", type="string", description="Empty = D-1 in Vietnam; demo source supports June 2026"
        ),
        "force_validation_failure": Param(
            False,
            type="boolean",
            description="Local smoke-test failure injection; never bypasses DQ",
        ),
        "validation_fail_once": Param(
            False,
            type="boolean",
            description="Local smoke test: fail first DQ attempt to verify retry",
        ),
    },
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=SyncCallback(deadline_exceeded),
    ),
    tags=["shopvn", "finance", "daily", "dq-gated"],
) as dag:
    begin = PythonOperator(task_id="initialize", python_callable=initialize)
    extract = PythonOperator(task_id="extract_snapshot", python_callable=extract_snapshot)
    transform = SparkSubmitOperator(
        task_id="transform_candidate",
        conn_id="spark_local",
        pool="spark_local",
        durable=False,
        application="/workspace/scripts/direct_sales_spark.py",
        driver_memory="1g",
        application_args=[
            "--snapshot-id",
            SNAPSHOT,
            "--candidate-name",
            "mart_fin02_" + KEY,
            "--result-path",
            RESULT,
            "--publication-id",
            KEY,
        ],
        conf={"spark.ui.enabled": "false"},
    )
    validate = PythonOperator(task_id="validate_candidate", python_callable=validate_candidate)
    publish = SparkSubmitOperator(
        task_id="publish_finance",
        conn_id="spark_local",
        pool="spark_local",
        durable=False,
        application="/workspace/scripts/publish_finance_spark.py",
        driver_memory="1g",
        application_args=["--run-key", KEY],
        conf={"spark.ui.enabled": "false"},
    )
    confirm = PythonOperator(task_id="verify_and_finish", python_callable=verify_and_finish)
    finish = PythonOperator(
        task_id="finalize", python_callable=finalize, trigger_rule="all_done", retries=0
    )
    begin >> extract >> transform >> validate >> publish >> confirm
    [begin, extract, transform, validate, publish, confirm] >> finish
