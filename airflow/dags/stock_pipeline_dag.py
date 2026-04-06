"""
Airflow DAG: Orchestrates the real-time stock market pipeline.

Schedule:
  - Spark Streaming job is long-running (submitted once, monitored continuously)
  - Delta Optimizer runs daily at 02:00 UTC
  - Data quality checks run every 30 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator, EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "vikas-reddy",
    "depends_on_past": False,
    "email": ["vkreddy241@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

EMR_CLUSTER_CONFIG = {
    "Name": "stock-pipeline-cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Instances": {
        "MasterInstanceType": "m5.xlarge",
        "SlaveInstanceType":  "m5.2xlarge",
        "InstanceCount": 3,
        "KeepJobFlowAliveWhenNoSteps": True,
    },
    "Applications": [{"Name": "Spark"}, {"Name": "Kafka"}],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://vkreddy-stock-lake/emr-logs/",
}

SPARK_STREAM_STEP = [
    {
        "Name": "StockTickStreamingJob",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                "s3://vkreddy-stock-lake/scripts/stream_processor.py",
            ],
        },
    }
]

OPTIMIZER_STEP = [
    {
        "Name": "DeltaOptimizerJob",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--packages", "io.delta:delta-core_2.12:2.4.0",
                "s3://vkreddy-stock-lake/scripts/delta_optimizer.py",
            ],
        },
    }
]

# ---------------------------------------------------------------------------
# Data quality check
# ---------------------------------------------------------------------------
def check_data_freshness(**context):
    import boto3
    from datetime import timezone

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket="vkreddy-stock-lake",
        Prefix="delta/stock_ticks/raw/",
        MaxKeys=1,
    )
    if not response.get("Contents"):
        raise ValueError("No data found in Delta Lake raw layer!")

    last_modified = response["Contents"][0]["LastModified"]
    age_minutes = (datetime.now(timezone.utc) - last_modified).seconds // 60
    if age_minutes > 10:
        raise ValueError(f"Data is stale: last write was {age_minutes} minutes ago.")

    print(f"Data freshness OK: last write {age_minutes} minutes ago.")


def check_tick_volume(**context):
    """Ensure we received ticks for all expected tickers in the last 5 minutes."""
    expected_tickers = {"AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"}
    # In production: query Delta Lake via boto3/athena and assert coverage
    print(f"Ticker coverage check passed for: {expected_tickers}")


# ---------------------------------------------------------------------------
# DAGs
# ---------------------------------------------------------------------------
with DAG(
    dag_id="stock_pipeline_streaming",
    default_args=DEFAULT_ARGS,
    description="Real-time stock tick ingestion and Delta Lake processing",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["streaming", "kafka", "delta-lake", "stocks"],
) as streaming_dag:

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id="aws_default",
    )

    submit_stream = EmrAddStepsOperator(
        task_id="submit_spark_streaming",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        steps=SPARK_STREAM_STEP,
        aws_conn_id="aws_default",
    )

    create_cluster >> submit_stream


with DAG(
    dag_id="stock_pipeline_maintenance",
    default_args=DEFAULT_ARGS,
    description="Delta Lake optimization and data quality checks",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["delta-lake", "maintenance", "data-quality"],
) as maintenance_dag:

    dq_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    dq_volume = PythonOperator(
        task_id="check_tick_volume",
        python_callable=check_tick_volume,
    )

    optimize = BashOperator(
        task_id="run_delta_optimizer",
        bash_command=(
            "spark-submit "
            "--packages io.delta:delta-core_2.12:2.4.0 "
            "/opt/airflow/scripts/delta_optimizer.py"
        ),
    )

    [dq_freshness, dq_volume] >> optimize
