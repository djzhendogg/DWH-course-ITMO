FROM ubuntu:22.04


ENV DEBIAN_FRONTEND=noninteractive
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin


# Адреса входа (фиксированные по условию задания)
ENV HDFS_URI=hdfs://192.168.34.2:8020
ENV YARN_RESOURCEMANAGER=192.168.34.2:8032


RUN apt-get update \
&& apt-get install -y --no-install-recommends \
wget curl ca-certificates openssh-client openjdk-11-jdk-headless procps \
&& rm -rf /var/lib/apt/lists/*


# Скачиваем и распаковываем Hadoop (клиентские утилиты)
RUN mkdir -p /opt && \
wget -qO /tmp/hadoop.tar.gz https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
tar -xzf /tmp/hadoop.tar.gz -C /opt && \
mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
rm /tmp/hadoop.tar.gz


# Настройки Hadoop клиента (простая конфигурация для обращения к удалённому кластеру)
COPY config/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
COPY config/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/yarn-site.xml


# Скрипты
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
COPY run_tests.sh /usr/local/bin/run_tests.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/run_tests.sh


WORKDIR /workspace


ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]