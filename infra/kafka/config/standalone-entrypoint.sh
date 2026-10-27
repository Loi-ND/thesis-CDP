#!/bin/bash
export KAFKA_CLUSTER_ID="$(cat /cluster.id)"
export DATA_DIR="/workspace/kafka/logs"

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  echo ">>> First time startup: formatting storage"
  kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c /workspace/kafka/config/server.properties
else
  echo ">>> Already formatted, skipping format"
fi

kafka-server-start.sh /workspace/kafka/config/server.properties #&

# # Chờ Kafka khởi động
# sleep 10

# # Chạy ingest
# python /workspace/streaming/_ingest_events.py >> /workspace/ingest.log 2>&1