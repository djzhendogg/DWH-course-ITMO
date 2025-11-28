FROM eclipse-temurin:8-jre-alpine

# Установка необходимых пакетов
RUN apk update && apk add --no-cache \
    wget \
    curl \
    python3 \
    bash


# Скачивание и установка Hadoop
ENV HADOOP_VERSION 3.3.6
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $HADOOP_HOME/bin:$PATH

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Копирование конфигурационных файлов
# COPY core-site.xml $HA/DOOP_CONF_DIR/
# COPY hdfs-site.xml $HADOOP_CONF_DIR/
# COPY mapred-site.xml $HADOOP_CONF_DIR/
# COPY yarn-site.xml $HADOOP_CONF_DIR/

# Копирование скрипта для выполнения задач
COPY run-tasks.sh /run-tasks.sh
RUN chmod +x /run-tasks.sh

WORKDIR /app

ENTRYPOINT ["/run-tasks.sh"]
