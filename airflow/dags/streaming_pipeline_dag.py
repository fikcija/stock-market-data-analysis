from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT_STREAMING = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 2g"
    " --executor-cores 1"
    " --num-executors 1"
    " --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
    " --jars /stream/consumer/postgresql-42.5.1.jar"
)

SPARK_SUBMIT_BASELINES = (
    "docker exec spark-master /spark/bin/spark-submit"
    " --master spark://spark-master:7077"
    " --driver-memory 1g"
    " --executor-memory 2g"
    " --executor-cores 2"
    " --num-executors 2"
    " --conf spark.sql.shuffle.partitions=8"
    " --conf spark.sql.adaptive.enabled=true"
)

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_streaming_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stock", "streaming"],
    default_args=DEFAULT_ARGS,
) as dag:

    load_baselines = BashOperator(
        task_id="load_streaming_baselines",
        bash_command=f"{SPARK_SUBMIT_BASELINES} /batch/jobs/11_load_streaming_baselines.py",
        execution_timeout=timedelta(minutes=30),
    )

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="docker start finnhub-producer",
        execution_timeout=timedelta(minutes=1),
    )

    sq1 = BashOperator(
        task_id="sq1_price_volatility",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/sq1_price_volatility.py",
        execution_timeout=timedelta(hours=12),
    )

    sq2 = BashOperator(
        task_id="sq2_market_sentiment",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/sq2_market_sentiment.py",
        execution_timeout=timedelta(hours=12),
    )

    sq3 = BashOperator(
        task_id="sq3_trade_frequency",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/sq3_trade_frequency.py",
        execution_timeout=timedelta(hours=12),
    )

    sq4 = BashOperator(
        task_id="sq4_price_vs_7d",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/sq4_price_vs_7d.py",
        execution_timeout=timedelta(hours=12),
    )

    sq5 = BashOperator(
        task_id="sq5_vs_prev_close",
        bash_command=f"{SPARK_SUBMIT_STREAMING} /stream/consumer/sq5_vs_prev_close.py",
        execution_timeout=timedelta(hours=12),
    )

    load_baselines >> run_producer >> [sq1, sq2, sq3, sq4, sq5]
