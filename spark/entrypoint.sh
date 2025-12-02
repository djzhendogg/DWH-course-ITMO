#!/bin/bash

# Функция для проверки доступности HDFS
check_hdfs_availability() {
    echo "Checking HDFS availability..."
    if hdfs dfs -ls / > /dev/null 2>&1; then
        echo "HDFS is available"
        return 0
    else
        echo "HDFS is not available"
        return 1
    fi
}

# Функция для ожидания подключения к Hadoop
wait_for_hadoop() {
    echo "Waiting for Hadoop to be available..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if check_hdfs_availability; then
            echo "Hadoop is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: Hadoop not ready, waiting 10 seconds..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "Hadoop not available after $max_attempts attempts"
    return 1
}

# Основной скрипт
echo "Starting Spark container..."

# Ожидание подключения к Hadoop
if ! wait_for_hadoop; then
    echo "Failed to connect to Hadoop. Exiting..."
    exit 1
fi

# Создание пустого файла на HDFS
echo "Creating empty file on HDFS..."
hdfs dfs -touchz /sparkExperiments.txt

# копирование датасетов
echo "Copy datasets..."
hdfs dfs -rm -r -f /ml-latest-small 2>/dev/null || true
hdfs dfs -put /data/ml-latest-small /

# Запуск Spark приложения
echo "Running Spark application..."
python /app/spark_app.py

echo "Spark application completed successfully!"