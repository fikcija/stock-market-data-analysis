#!/bin/bash

JAR=/stream/consumer/postgresql-42.5.1.jar
SUBMIT="/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  --jars $JAR"

echo "=== SQ1: Intraday price volatility (10-min window) ==="
$SUBMIT /stream/consumer/sq1_price_volatility.py &

echo "=== SQ2: Market sentiment per exchange (1-min window) ==="
$SUBMIT /stream/consumer/sq2_market_sentiment.py &

echo "=== SQ3: Trade frequency vs baseline (10-min window) ==="
$SUBMIT /stream/consumer/sq3_trade_frequency.py &

echo "=== SQ4: Price vs 7-day high/low (5-min window) ==="
$SUBMIT /stream/consumer/sq4_price_vs_7d.py &

echo "=== SQ5: Price deviation from previous close (1-min window) ==="
$SUBMIT /stream/consumer/sq5_vs_prev_close.py &

echo "=== All 5 streaming consumers started in background ==="
wait
echo "=== All streaming consumers done ==="
