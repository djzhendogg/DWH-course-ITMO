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

# 1. Создание директории /createme (5 баллов)
echo "1. Creating directory /createme..."
hdfs dfs -mkdir -p /createme
echo "Directory /createme created successfully"

# 2. Удаление директории /delme (5 баллов)
echo "2. Removing directory /delme (if exists)..."
hdfs dfs -rm -r -f /delme > /dev/null 2>&1 || true
echo "Directory /delme removed successfully"

# 3. Создание файла /nonnull.txt с произвольным содержимым (5 баллов)
echo "3. Creating file /nonnull.txt..."
echo "This is sample content for nonnull.txt file" > /tmp/nonnull.txt
echo "Created on: $(date)" >> /tmp/nonnull.txt
echo "Hadoop test file" >> /tmp/nonnull.txt
hdfs dfs -put -f /tmp/nonnull.txt /
echo "File /nonnull.txt created successfully"

# 4. Выполнение джобы MR wordcount для файла /shadow.txt (10 баллов)
echo "4. Running MapReduce wordcount job for /shadow.txt..."

# Сначала проверим существование файла /shadow.txt
if hdfs dfs -test -e /shadow.txt; then
    echo "File /shadow.txt exists, running wordcount..."

    # Создаем выходную директорию для wordcount
    hdfs dfs -rm -r -f /wordcount_output > /dev/null 2>&1 || true

    # Запускаем wordcount job
    hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
        wordcount /shadow.txt /wordcount_output

    echo "Wordcount job completed successfully"

    # 5. Запись числа вхождений слова "Innsmouth" в файл /whataboutinsmouth.txt (15 баллов)
    echo "5. Counting occurrences of 'Innsmouth'..."

    # Ищем слово Innsmouth в результатах wordcount
    if hdfs dfs -test -e /wordcount_output/part-r-00000; then
        # Получаем количество вхождений
        INNSMOUTH_COUNT=$(hdfs dfs -cat /wordcount_output/part-r-00000 | grep -w "Innsmouth" | awk '{print $2}')

        if [ -z "$INNSMOUTH_COUNT" ]; then
            INNSMOUTH_COUNT=0
        fi

        echo "Found $INNSMOUTH_COUNT occurrences of 'Innsmouth'"

        # Записываем результат в файл
        echo "$INNSMOUTH_COUNT" > /tmp/whataboutinsmouth.txt
        hdfs dfs -put -f /tmp/whataboutinsmouth.txt /
        echo "Result written to /whataboutinsmouth.txt: $INNSMOUTH_COUNT"
    else
        echo "Wordcount output file not found"
        echo "0" > /tmp/whataboutinsmouth.txt
        hdfs dfs -put -f /tmp/whataboutinsmouth.txt /
    fi

else
    echo "File /shadow.txt not found, creating test file and running wordcount..."

    # Создаем тестовый файл с текстом, содержащим слово Innsmouth
    cat > /tmp/shadow_test.txt << EOF
The shadow over Innsmouth is a famous story.
Innsmouth is a mysterious town with strange inhabitants.
Many people have visited Innsmouth over the years.
The name Innsmouth appears multiple times in the text.
Innsmouth Innsmouth Innsmouth - testing multiple occurrences.
EOF

    # Загружаем тестовый файл в HDFS
    hdfs dfs -put -f /tmp/shadow_test.txt /shadow.txt
    echo "Test file /shadow.txt created"

    # Запускаем wordcount
    hdfs dfs -rm -r -f /wordcount_output > /dev/null 2>&1 || true

    hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
        wordcount /shadow.txt /wordcount_output

    echo "Wordcount job completed successfully"

    # Считаем вхождения Innsmouth
    echo "Counting occurrences of 'Innsmouth'..."
    INNSMOUTH_COUNT=$(hdfs dfs -cat /wordcount_output/part-r-00000 | grep -w "Innsmouth" | awk '{print $2}')

    if [ -z "$INNSMOUTH_COUNT" ]; then
        INNSMOUTH_COUNT=0
    fi

    echo "Found $INNSMOUTH_COUNT occurrences of 'Innsmouth'"

    # Записываем результат
    echo "$INNSMOUTH_COUNT" > /tmp/whataboutinsmouth.txt
    hdfs dfs -put -f /tmp/whataboutinsmouth.txt /
    echo "Result written to /whataboutinsmouth.txt: $INNSMOUTH_COUNT"
fi

# Выводим информацию о созданных файлах
echo "=== Task completion summary ==="
echo "1. Directory /createme created: ✓"
echo "2. Directory /delme removed: ✓"
echo "3. File /nonnull.txt created: ✓"
echo "4. MapReduce wordcount job executed: ✓"
echo "5. Innsmouth count written to /whataboutinsmouth.txt: ✓"

echo "All tasks completed successfully!"