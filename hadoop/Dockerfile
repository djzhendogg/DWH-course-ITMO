FROM eclipse-temurin:8-jre-alpine

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Скачивание и установка Hadoop
ENV HADOOP_VERSION 3.3.6
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $HADOOP_HOME/bin:$PATH

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Создание необходимых директорий
RUN mkdir -p /tmp/hadoop-root/dfs/name && \
    mkdir -p /tmp/hadoop-root/dfs/data

# Копирование конфигурационных файлов
COPY core-site.xml $HADOOP_CONF_DIR/
COPY hdfs-site.xml $HADOOP_CONF_DIR/
COPY mapred-site.xml $HADOOP_CONF_DIR/
COPY yarn-site.xml $HADOOP_CONF_DIR/

# Копирование скрипта для выполнения задач
COPY run-tasks.sh /run-tasks.sh
RUN chmod +x /run-tasks.sh

# Создание тестового файла для wordcount
COPY test-data.txt /test-data.txt

WORKDIR /app

ENTRYPOINT ["/run-tasks.sh"]