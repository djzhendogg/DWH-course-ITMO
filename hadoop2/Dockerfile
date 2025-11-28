FROM eclipse-temurin:8-jre-alpine

# Установка необходимых пакетов
RUN apk update && apk add --no-cache \
    wget \
    curl \
    bash \
    tar \
    coreutils

# Переменные
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${HADOOP_HOME}/bin:${PATH}

# Скачивание и распаковка Hadoop
WORKDIR /tmp
RUN wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz" && \
    mv "hadoop-${HADOOP_VERSION}" ${HADOOP_HOME} && \
    rm "hadoop-${HADOOP_VERSION}.tar.gz"

# Копируем конфиги (они будут содержать правильные адреса HDFS/YARN)
COPY config/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY config/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY config/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml

# Копирование скрипта для выполнения задач
COPY run-tasks.sh /run-tasks.sh
RUN chmod +x /run-tasks.sh

WORKDIR /app

ENTRYPOINT ["/run-tasks.sh"]
