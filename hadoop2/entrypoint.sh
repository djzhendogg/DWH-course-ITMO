#!/usr/bin/env bash
set -euo pipefail


# Подставляем реальные URIs в конфиги
HDFS_URI=${HDFS_URI:-hdfs://192.168.34.2:8020}
YARN_RM=${YARN_RESOURCEMANAGER:-192.168.34.2:8032}


# Простая подстановка в файлы конфигурации
sed -e "s|${hdfs.uri}|${HDFS_URI}|g" \
-e "s|${yarn.rm}|${YARN_RM}|g" \
${HADOOP_HOME}/etc/hadoop/core-site.xml.template >/tmp/core-site.xml || true


# Если в образе конфиги уже были скопированы — перезапишем файлы каталога
if [ -f /tmp/core-site.xml ]; then
mv /tmp/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
fi


# Заменяем yarn-site.xml аналогично
if grep -q "${yarn.rm}" ${HADOOP_HOME}/etc/hadoop/yarn-site.xml 2>/dev/null; then
true
else
sed -e "s|${yarn.rm}|${YARN_RM}|g" ${HADOOP_HOME}/etc/hadoop/yarn-site.xml >/tmp/yarn-site.xml || true
if [ -f /tmp/yarn-site.xml ]; then
mv /tmp/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
fi
fi


# Выводим краткую информацию
echo "HADOOP_HOME=${HADOOP_HOME}"
echo "Using HDFS_URI=${HDFS_URI}"
echo "Using YARN_RM=${YARN_RM}"


# Запускаем набор тестов
exec /usr/local/bin/run_tests.sh