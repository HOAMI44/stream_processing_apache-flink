FROM flink:2.2.0-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2

ENV PYTHONDONTWRITEBYTECODE=1

USER root

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-dev python3-pip curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && python3 -m pip install --break-system-packages --no-cache-dir apache-flink==2.2.0 \
    && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    && rm -rf /var/lib/apt/lists/*

USER flink
