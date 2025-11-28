#!/usr/bin/env bash
set -euo pipefail

echo "Starting Hadoop tasks..."

export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export PATH=${HADOOP_HOME}/bin:${PATH}

HDFS_CMD="hdfs dfs"
HADOOP_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION:-3.3.6}.jar"

# 0) Проверка: покажем конфиг (полезно для дебага)
echo "Using HADOOP_CONF_DIR=${HADOOP_CONF_DIR}"
echo "Contents of core-site.xml:"
cat ${HADOOP_CONF_DIR}/core-site.xml || true
echo "Contents of yarn-site.xml:"
cat ${HADOOP_CONF_DIR}/yarn-site.xml || true

######################################
#  WAIT FOR HDFS (пока fs доступен)
######################################
check_hdfs() {
    # пробуем выполнить простую операцию ls корня на HDFS
    ${HDFS_CMD} -ls / >/dev/null 2>&1
}

echo "Waiting for HDFS..."
for i in $(seq 1 30); do
    if check_hdfs; then
        echo "HDFS is available."
        break
    fi
    echo "HDFS not ready, retry $i/30..."
    sleep 2
    if [ "$i" -eq 30 ]; then
        echo "ERROR: HDFS did not become available in time" >&2
        exit 1
    fi
done

######################################
#  TASK 1: CREATE /createme
######################################
echo "1. Creating /createme ..."
${HDFS_CMD} -mkdir -p /createme
if ${HDFS_CMD} -test -d /createme ; then
    echo "✓ Directory /createme created"
else
    echo "ERROR: failed to create /createme" >&2
    exit 1
fi

######################################
#  TASK 2: REMOVE /delme
######################################
echo "2. Removing /delme ..."
# Удаляем путь на HDFS (обязательно с ведущим слэшем)
${HDFS_CMD} -rm -r -f /delme >/dev/null 2>&1 || true
echo "=== HDFS root directory ==="
${HDFS_CMD} -ls / || true
echo "==========================="
# double-check
if ${HDFS_CMD} -test -e /delme; then
    echo "ERROR: /delme still exists!" >&2
    exit 1
else
    echo "✓ Directory /delme removed (or did not exist)"
fi

######################################
#  TASK 3: CREATE /nonnull.txt
######################################
echo "3. Creating /nonnull.txt ..."
cat > /tmp/nonnull.txt <<'EOF'
This is a non-empty file.
Created automatically by run-tasks.sh.
EOF

${HDFS_CMD} -put -f /tmp/nonnull.txt /nonnull.txt
if ${HDFS_CMD} -test -e /nonnull.txt ; then
    echo "✓ File /nonnull.txt created"
else
    echo "ERROR: failed to create /nonnull.txt" >&2
    exit 1
fi
rm -f /tmp/nonnull.txt

######################################
#  TASK 4: RUN WORDCOUNT ON /shadow.txt
######################################
echo "4. Running wordcount on /shadow.txt ..."

# Проверяем строго с ведущим слэшем
if ! ${HDFS_CMD} -test -e /shadow.txt; then
    echo "ERROR: /shadow.txt does not exist in HDFS!" >&2
    exit 1
fi

# Удалим старый выход
OUT_DIR=/wordcount_output
${HDFS_CMD} -rm -r -f ${OUT_DIR} >/dev/null 2>&1 || true

# Запуск MR через hadoop jar (yarn)
echo "Launching MR job (this will use YARN RM from yarn-site.xml)..."
hadoop jar ${HADOOP_JAR} wordcount /shadow.txt ${OUT_DIR}

# Проверка результата
if ${HDFS_CMD} -test -d ${OUT_DIR}; then
    echo "✓ Wordcount job finished, output at ${OUT_DIR}"
else
    echo "ERROR: Wordcount did not produce output directory ${OUT_DIR}" >&2
    exit 1
fi

######################################
#  TASK 5: COUNT EXACT 'Innsmouth' (точно такого регистра) AND WRITE FILE
######################################
echo "5. Extracting count of Innsmouth ..."

# Собираем все part-* (если несколько файлов) и ищем строгое совпадение "Innsmouth" в начале строки
TMP_OUT=/tmp/wordcount_combined.txt
${HDFS_CMD} -cat ${OUT_DIR}/part-* > ${TMP_OUT} || true

# Ищем строку, начинающуюся с точно "Innsmouth" (регистр важен) и берём сумму по всем part-файлам
INNSMOUTH_COUNT=$(grep -P '^Innsmouth\s+' ${TMP_OUT} | awk '{sum += $2} END{print sum+0}')

# Если не нашли — положим 0
if [ -z "${INNSMOUTH_COUNT}" ] || [ "${INNSMOUTH_COUNT}" = "" ]; then
    INNSMOUTH_COUNT=0
    echo "Warning: 'Innsmouth' not found in wordcount output; writing 0"
else
    echo "Found Innsmouth -> ${INNSMOUTH_COUNT}"
fi

# Записываем одно числовое значение в HDFS /whataboutinsmouth.txt
echo "${INNSMOUTH_COUNT}" > /tmp/whataboutinsmouth.txt
${HDFS_CMD} -put -f /tmp/whataboutinsmouth.txt /whataboutinsmouth.txt
if ${HDFS_CMD} -test -e /whataboutinsmouth.txt; then
    echo "✓ Result saved to /whataboutinsmouth.txt (value: ${INNSMOUTH_COUNT})"
else
    echo "ERROR: failed to write /whataboutinsmouth.txt" >&2
    exit 1
fi
rm -f /tmp/wordcount_combined.txt /tmp/whataboutinsmouth.txt

echo "=== All tasks completed successfully ==="
