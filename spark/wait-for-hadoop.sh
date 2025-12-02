#!/bin/bash
# wait-for-hadoop.sh

HOST=${HADOOP_HOST:-192.155.34.2}
HDFS_PORT=${HDFS_PORT:-8020}
YARN_PORT=${YARN_PORT:-8032}
MAX_ATTEMPTS=30
ATTEMPT=1

echo "Waiting for Hadoop cluster to be ready..."

# Проверка HDFS
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Checking HDFS..."

    # Проверка порта HDFS
    if nc -z $HOST $HDFS_PORT; then
        echo "HDFS port is open"

        # Проверка доступности HDFS
        if hdfs dfs -ls / > /dev/null 2>&1; then
            echo "HDFS is fully available"
            break
        fi
    fi

    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "Hadoop not available after $MAX_ATTEMPTS attempts"
        exit 1
    fi

    echo "Waiting 10 seconds..."
    sleep 10
    ATTEMPT=$((ATTEMPT + 1))
done

# Проверка YARN
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Checking YARN ResourceManager..."

    if nc -z $HOST $YARN_PORT; then
        echo "YARN ResourceManager is available"
        exit 0
    fi

    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "YARN not available after $MAX_ATTEMPTS attempts"
        exit 1
    fi

    echo "Waiting 5 seconds..."
    sleep 5
    ATTEMPT=$((ATTEMPT + 1))
done