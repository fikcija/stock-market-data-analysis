#!/bin/bash
# Register Airflow connections for the stock market project

airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077 \
    || echo "spark_default already exists"

airflow connections add postgres_stock_analytics \
    --conn-type postgres \
    --conn-host pg \
    --conn-port 5432 \
    --conn-schema stock_analytics \
    --conn-login postgres \
    --conn-password postgres \
    || echo "postgres_stock_analytics already exists"
