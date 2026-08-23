#!/usr/bin/env bash
set -e

# 1. Use Java from .venv
if ! command -v java &> /dev/null; then
    JAVA_BIN=$(find "$(pwd)/.venv" -type f \( -name "java" -o -name "java.exe" \) 2>/dev/null | head -n 1)
    if [ -n "$JAVA_BIN" ]; then
        export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
        export PATH="${JAVA_HOME}/bin:${PATH}"
        echo "Using JDK from .venv: ${JAVA_HOME}"
    else
        echo "Error: Java not found."
        exit 1
    fi
fi

cd kafka_2.13-3.8.0

# 2. Format KRaft storage using random-uuid
if [ ! -d "/tmp/kraft-combined-logs" ]; then
    echo "Formatting KRaft storage..."
    CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format -t "$CLUSTER_ID" -c config/kraft/server.properties
fi

# 3. Start Kafka
echo "Starting Kafka on localhost:9092..."
exec bin/kafka-server-start.sh config/kraft/server.properties