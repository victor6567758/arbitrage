###############################################################################
# Build PyFlink Playground Image
###############################################################################

FROM apache/flink:1.13.1-scala_2.12-java8
ARG FLINK_VERSION=1.13.1

# Install pyflink
RUN set -ex; \
  cat /etc/os-release;  \
  apt-get update; \
  cd /opt;  \
  apt-get install -y make build-essential checkinstall git-core curl zlib1g-dev build-essential \
    libssl-dev libreadline-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt1-dev libcurl4-openssl-dev liblzma-dev libffi-dev \
    libncursesw5-dev libssl-dev libsqlite3-dev xz-utils tk-dev libgdbm-dev libc6-dev libbz2-dev;  \
  wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz; \
  tar xvf Python-3.6.9.tgz; \
  cd Python-3.6.9; \
  ./configure; \
  make altinstall; \
  ln -sf /opt/Python-3.6.9/python /usr/bin/python;  \
  python -c 'import sys; print(sys.version_info[:])';\
  python -m pip install apache-flink==1.13.1; \
  python -m pip install kafka-python;


# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/${FLINK_VERSION}/flink-sql-connector-kafka_2.12-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.12/${FLINK_VERSION}/flink-sql-connector-elasticsearch7_2.12-${FLINK_VERSION}.jar;


RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;

WORKDIR /opt/flink
