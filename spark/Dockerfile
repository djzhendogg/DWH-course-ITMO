FROM python:3.9-bookworm

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    procps \
    gnupg \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN java -version

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
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

WORKDIR /app
# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Настройка Hadoop для работы с HDFS
COPY core-site.xml ${HADOOP_CONF_DIR}/
COPY hdfs-site.xml ${HADOOP_CONF_DIR}/

# Копирование файлов приложения
COPY data .
COPY spark_app.py .
COPY entrypoint.sh .

# Создание пользователя и настройка разрешений
RUN chmod +x entrypoint.sh
RUN ln -s /usr/bin/python3 /usr/bin/python

ENTRYPOINT ["./entrypoint.sh"]