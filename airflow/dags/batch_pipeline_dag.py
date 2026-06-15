from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 1000m"
    " --executor-cores 1"
    " --num-executors 2"
    " --conf spark.sql.shuffle.partitions=50"
    " --conf spark.default.parallelism=50"
    " --conf spark.memory.fraction=0.6"
    " --conf spark.memory.storageFraction=0.3"
    " --conf spark.sql.parquet.writeLegacyFormat=true"
    " --conf spark.sql.adaptive.enabled=true"
    " --conf spark.sql.adaptive.coalescePartitions.enabled=true"
)

SPARK_SUBMIT_PG = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 1000m"
    " --executor-cores 1"
    " --num-executors 2"
    " --conf spark.sql.shuffle.partitions=50"
    " --conf spark.default.parallelism=50"
    " --conf spark.memory.fraction=0.6"
    " --conf spark.memory.storageFraction=0.3"
    " --conf spark.sql.adaptive.enabled=true"
    " --jars /batch/lib/postgresql-42.5.1.jar"
)

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_batch_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stock", "batch"],
    default_args=DEFAULT_ARGS,
) as dag:

    load_raw = BashOperator(
        task_id="load_raw_layer",
        bash_command=f"{SPARK_SUBMIT} /batch/jobs/01_load_raw_layer.py",
        execution_timeout=timedelta(hours=2),
    )

    process_prices = BashOperator(
        task_id="process_stock_prices",
        bash_command=f"{SPARK_SUBMIT} /batch/jobs/02_process_stock_prices.py",
        execution_timeout=timedelta(hours=1),
    )

    daily_metrics = BashOperator(
        task_id="daily_stock_metrics",
        bash_command=f"{SPARK_SUBMIT} /batch/jobs/03_daily_stock_metrics.py",
        execution_timeout=timedelta(hours=2),
    )

    monthly_perf = BashOperator(
        task_id="monthly_stock_performance",
        bash_command=f"{SPARK_SUBMIT} /batch/jobs/04_monthly_stock_performance.py",
        execution_timeout=timedelta(hours=2),
    )

    yearly_perf = BashOperator(
        task_id="yearly_stock_performance",
        bash_command=f"{SPARK_SUBMIT} /batch/jobs/05_yearly_stock_performance.py",
        execution_timeout=timedelta(hours=2),
    )

    export_pg = BashOperator(
        task_id="export_to_postgres",
        bash_command=f"{SPARK_SUBMIT_PG} /batch/jobs/06_export_to_postgres.py",
        execution_timeout=timedelta(hours=1),
    )

    load_raw >> process_prices >> daily_metrics >> monthly_perf >> yearly_perf >> export_pg
