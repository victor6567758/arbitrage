###############################################################################
# Build PyFlink Playground Image
###############################################################################

FROM apache/flink:1.13.1-scala_2.12-java8
ARG FLINK_VERSION=1.13.1
ARG FLINK_LIB=/opt/flink

WORKDIR /opt
COPY ./binaries/python-3.6.9-bin.tar.gz .


RUN set -ex; \
  cat /etc/os-release;  \
  apt-get update; \
  tar xvf python-3.6.9-bin.tar.gz;  \
  ln -sf /opt/Python-3.6.9/python /usr/bin/python;  \
  python -c 'import sys; print(sys.version_info[:])';\
  rm -f python-3.6.9-bin.tar.gz;  \
  wget -P . https://bootstrap.pypa.io/get-pip.py; \
  python get-pip.py;  \
  python -m pip --version;  \
  apt-get -y install python3-dev; \
  apt-get update; \
  python -m pip install apache-flink==$FLINK_VERSION; \
  python -m pip install kafka-python;



# Download connector libraries
RUN wget -P ${FLINK_LIB}/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/${FLINK_VERSION}/flink-sql-connector-kafka_2.12-${FLINK_VERSION}.jar; \
    wget -P ${FLINK_LIB}/lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.12/${FLINK_VERSION}/flink-sql-connector-elasticsearch7_2.12-${FLINK_VERSION}.jar;


RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> ${FLINK_LIB}/conf/flink-conf.yaml;

WORKDIR /opt/flink
