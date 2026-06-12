#!/bin/bash

JAR=/stream/consumer/postgresql-42.5.1.jar
SUBMIT="/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  --jars $JAR \
  --executor-memory 512m \
  --driver-memory 512m \
  --conf spark.sql.shuffle.partitions=4"

run_consumer() {
    local name=$1
    local script=$2
    $SUBMIT $script
    echo "=== $name exited with code $? ===" >&2
}

echo "=== SQ1: Intraday price volatility (10-min window) ==="
run_consumer "SQ1" /stream/consumer/sq1_price_volatility.py &
PIDS[1]=$!

echo "=== SQ2: Market sentiment per exchange (1-min window) ==="
run_consumer "SQ2" /stream/consumer/sq2_market_sentiment.py &
PIDS[2]=$!

echo "=== SQ3: Trade frequency vs baseline (10-min window) ==="
run_consumer "SQ3" /stream/consumer/sq3_trade_frequency.py &
PIDS[3]=$!

echo "=== SQ4: Price vs 7-day high/low (5-min window) ==="
run_consumer "SQ4" /stream/consumer/sq4_price_vs_7d.py &
PIDS[4]=$!

echo "=== SQ5: Price deviation from previous close (1-min window) ==="
run_consumer "SQ5" /stream/consumer/sq5_vs_prev_close.py &
PIDS[5]=$!

echo "=== All 5 streaming consumers started (PIDs: ${PIDS[*]}) ==="
wait
echo "=== All streaming consumers have exited ==="
