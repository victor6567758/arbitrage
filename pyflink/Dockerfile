###############################################################################
# Build PyFlink Playground Image
###############################################################################
FROM apache/flink:1.14.3-scala_2.11-java8

WORKDIR /opt
COPY ./binaries/python-3.6.9-bin.tar.gz .

RUN set -ex; \
  cat /etc/os-release;  \
  apt-get update; \
  tar -xf python-3.6.9-bin.tar.gz;  \
  ln -sf /opt/Python-3.6.9/python /usr/bin/python;  \
  python -c 'import sys; print(sys.version_info[:])';\
  rm -f python-3.6.9-bin.tar.gz;  \
  wget -P . https://bootstrap.pypa.io/get-pip.py; \
  python get-pip.py;  \
  python -m pip --version;  \
  apt-get -y install python3-dev; \
  apt-get update; \
  python -m pip install apache-flink==1.14.3; \
  python -m pip install kafka-python==2.0.2;



WORKDIR /opt/flink
COPY ./binaries/flink-sql-connector-elasticsearch7_2.11-1.14.3.jar ./lib
COPY ./binaries/flink-sql-connector-kafka_2.11-1.14.3.jar ./lib

# Download connector libraries
#RUN wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar; \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/${FLINK_VERSION}/flink-csv-${FLINK_VERSION}.jar; \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka_2.12/${FLINK_VERSION}/flink-connector-kafka_2.12-${FLINK_VERSION}.jar; \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/${FLINK_VERSION}/flink-sql-connector-kafka_2.12-${FLINK_VERSION}.jar; \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-elasticsearch7_2.12/${FLINK_VERSION}/flink-connector-elasticsearch7_2.12-${FLINK_VERSION}.jar;\
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.12/${FLINK_VERSION}/flink-sql-connector-elasticsearch7_2.12-${FLINK_VERSION}.jar;\
#    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.1.0/kafka-clients-3.1.0.jar; \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/apache/httpcomponents/httpcore/4.4.15/httpcore-4.4.15.jar;  \
#    wget -P /opt/flink/lib https://repo.maven.apache.org/maven2/org/elasticsearch/elasticsearch-x-content/6.7.2/elasticsearch-x-content-6.7.2.jar;

RUN chown flink:flink /opt/flink/lib/*
RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;


