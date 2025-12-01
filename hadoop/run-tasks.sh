#!/bin/bash

set -e

echo "Starting Hadoop tasks..."

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$PATH

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

check_test_availability() {
    echo "Checking shadow.txt availability..."
    if hdfs dfs -test -e /shadow.txt; then
        echo "shadow.txt is available"
        return 0
    else
        echo "shadow.txt is not available"
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

wait_for_test() {
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if check_test_availability; then
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: TEST not ready, waiting 10 seconds..."
        sleep 10
        attempt=$((attempt + 1))
    done

    echo "TEST is not available after $max_attempts attempts"
    return 1
}

# Ожидаем доступности HDFS
wait_for_hdfs

wait_for_test

echo "=== Starting HDFS tasks ==="

echo "=== HDFS root directory ==="
hdfs dfs -ls /
echo "==========================="

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

if ! hdfs dfs -test -e /shadow.txt; then
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
#INNSMOUTH_COUNT=$(hdfs dfs -cat /wordcount_output/part-r-00000 \
#    | grep -i -w "Innsmouth" \
#    | awk '{sum += $2} END{print sum+0}')
INNSMOUTH_COUNT=$(hdfs dfs -cat /wordcount_output/part-r-00000 | grep -w "Innsmouth" | awk '{print $2}')

echo "Found $INNSMOUTH_COUNT occurrences of Innsmouth"

# Write result
echo "$INNSMOUTH_COUNT" > /tmp/whataboutinsmouth.txt
hdfs dfs -put -f /tmp/whataboutinsmouth.txt /whataboutinsmouth.txt

echo "✓ Result saved to /whataboutinsmouth.txt"

######################################
echo "=== All tasks completed successfully ==="
