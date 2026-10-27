#!/bin/bash
export CLUSTER_ID="$(cat /cluster.id)"
export CONTROLLER_1_UUID="$(cat /workspace/kafka/server-uuid/server-1.id)"\
export DATA_DIR="/workspace/kafka/logs"

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  echo ">>> First time startup: formatting storage"
  kafka-storage.sh format \
    --cluster-id "${CLUSTER_ID}" \
    --initial-controllers "1@kafka-server-1:9094:${CONTROLLER_1_UUID}" \
    --config /workspace/kafka/config/server.properties
else
  echo ">>> Already formatted, skipping format"
fi

kafka-server-start.sh /workspace/kafka/config/server.properties