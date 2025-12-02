FROM python:3.9-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    openjdk-11-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Установка Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Установка Spark
ENV SPARK_VERSION=3.3.2
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Настройка переменных окружения
ENV PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Настройка Hadoop для работы с HDFS
COPY core-site.xml ${HADOOP_CONF_DIR}/
COPY hdfs-site.xml ${HADOOP_CONF_DIR}/

# Копирование файлов приложения
WORKDIR /app
COPY requirements.txt .
COPY spark_app.py .
COPY entrypoint.sh .
COPY wait-for-hadoop.sh .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Создание пользователя и настройка разрешений
RUN chmod +x entrypoint.sh wait-for-hadoop.sh

ENTRYPOINT ["./entrypoint.sh"]