#!/bin/bash

set -e

echo "Starting Hadoop tasks..."

export HADOOP_HOME=/opt/hadoop
export $HDFS_ENDPOINT=hdfs://192.168.34.2:8020
export WAIT_TIMEOUT=5000
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$PATH

echo "Waiting for HDFS to be ready at $HDFS_ENDPOINT..."
timeout $WAIT_TIMEOUT bash -c '
  until hdfs dfs -fs $0 -test -d / &>/dev/null; do
    echo "HDFS not ready, waiting 5 seconds..."
    sleep 10
  done
' $HDFS_ENDPOINT
echo "HDFS is ready. Starting tasks."

# --- СИНХРОНИЗАЦИЯ С ТЕСТОВОЙ СИСТЕМОЙ ---
# Ждем, пока тестовая система закончит свою подготовку (создаст файл /shadow.txt)
echo "Waiting for input file /shadow.txt to appear..."
timeout $WAIT_TIMEOUT bash -c '
  until hdfs dfs -fs $0 -test -e /shadow.txt &>/dev/null; do
    echo "File /shadow.txt not found, waiting 5 seconds..."
    sleep 10
  done
' $HDFS_ENDPOINT
echo "Test system setup is complete."

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

# Функция для ожидания доступности HDFS
wait_for_hdfs() {
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if check_hdfs_availability; then
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: HDFS not ready, waiting 10 seconds..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "HDFS is not available after $max_attempts attempts"
    return 1
}

# Ожидаем доступности HDFS
wait_for_hdfs

echo "=== Starting HDFS tasks ==="

echo "=== HDFS root directory ==="
hdfs dfs -ls /
echo "==========================="

if hdfs dfs -test -e /app; then
    echo "=== HDFS app directory ==="
    hdfs dfs -ls /app
    echo "==========================="
fi
if hdfs dfs -test -e /app; then
    echo "=== HDFS opt/hadoop directory ==="
    hdfs dfs -ls /opt/hadoop
    echo "==========================="
fi
if hdfs dfs -test -e /root; then
    echo "=== HDFS root directory ==="
    hdfs dfs -ls /root
    echo "==========================="
fi
if hdfs dfs -test -e /tmp; then
echo "=== HDFS tmp directory ==="
hdfs dfs -ls /tmp
echo "==========================="
fi
######################################
#  TASK 1: CREATE /createme
######################################
echo "1. Creating /createme ..."
hdfs dfs -mkdir -p /createme
echo "✓ Directory /createme created"

######################################
#  TASK 2: REMOVE /delme
######################################

echo "2. Removing /delme ..."
hdfs dfs -rm -r -f /delme >/dev/null 2>&1 || true
# double-check
if hdfs dfs -test -e /delme; then
    echo "ERROR: /delme still exists!"
else
    echo "✓ Directory /delme removed"
fi

######################################
#  TASK 3: CREATE /nonnull.txt
######################################
echo "3. Creating /nonnull.txt ..."

cat > /tmp/nonnull.txt <<EOF
This is a non-empty file.
Created automatically by run-tasks.sh.
EOF

hdfs dfs -put -f /tmp/nonnull.txt /nonnull.txt
echo "✓ File /nonnull.txt created"

######################################
#  TASK 4: RUN WORDCOUNT ON /shadow.txt
######################################
echo "4. Running wordcount on /shadow.txt ..."

if ! hdfs dfs -test -e shadow.txt; then
    echo "ERROR: /shadow.txt does not exist in HDFS!"
    exit 1
fi

# Remove old output
hdfs dfs -rm -r -f /wordcount_output >/dev/null 2>&1 || true

# Run job
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    wordcount /shadow.txt /wordcount_output

echo "✓ Wordcount job finished"

######################################
#  TASK 5: COUNT INNSMOUTH AND WRITE FILE
######################################
echo "5. Extracting count of Innsmouth ..."

if ! hdfs dfs -test -e /wordcount_output/part-r-00000; then
    echo "ERROR: No output file found!"
    exit 1
fi

# sum all case-insensitive occurrences
INNSMOUTH_COUNT=$(hdfs dfs -cat /wordcount_output/part-r-00000 \
    | grep -i -w "Innsmouth" \
    | awk '{sum += $2} END{print sum+0}')

echo "Found $INNSMOUTH_COUNT occurrences of Innsmouth"

# Write result
echo "$INNSMOUTH_COUNT" > /tmp/whataboutinsmouth.txt
hdfs dfs -put -f /tmp/whataboutinsmouth.txt /whataboutinsmouth.txt

echo "✓ Result saved to /whataboutinsmouth.txt"

######################################
echo "=== All tasks completed successfully ==="
