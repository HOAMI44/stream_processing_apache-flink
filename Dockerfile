FROM flink:1.20.1-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG JDBC_CONNECTOR_VERSION=3.3.0-1.20
ARG POSTGRES_JDBC_VERSION=42.7.12

ENV PYTHONDONTWRITEBYTECODE=1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER root

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless python3 python3-dev python3-pip curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && python3 -m pip install --no-cache-dir apache-flink==1.20.1 \
    && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    && curl -fsSL -o /opt/flink/lib/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar \
    && curl -fsSL -o /opt/flink/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    && rm -rf /var/lib/apt/lists/*

USER flink
