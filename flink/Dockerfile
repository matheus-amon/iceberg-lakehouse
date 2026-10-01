# Flink with the connectors the job needs, plus the Hadoop client classes that
# the Iceberg `hadoop` catalog type requires at runtime.
#
# The connectors are copied in here rather than resolved via `pipeline.jars` at
# submit time, so `docker compose up` works without Maven Central being
# reachable from inside the compose network.
#
# There is no PyFlink image variant for 1.20.1, so the job is written in Flink
# SQL and submitted through the SQL client instead.
FROM apache/flink:1.20.1-scala_2.12-java17

ARG ICEBERG_VERSION=1.7.1
ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20
ARG HADOOP_VERSION=3.3.6

USER root

# Iceberg's `catalog-type = hadoop` path constructs an
# org.apache.hadoop.conf.Configuration to resolve the warehouse URI. Flink's
# slim image ships no Hadoop classes, so without these the job dies with a bare
# ClassNotFoundException the moment it touches the catalog.
RUN set -eux; \
    mvn_base="https://repo1.maven.org/maven2"; \
    get() { curl -fsSL -o "/opt/flink/lib/$2" "$mvn_base/$1"; }; \
    get "org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar" iceberg-flink-runtime.jar; \
    get "org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" flink-sql-connector-kafka.jar; \
    get "org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" hadoop-common.jar; \
    get "org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar" hadoop-hdfs-client.jar; \
    get "org/apache/hadoop/hadoop-mapreduce-client-core/${HADOOP_VERSION}/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar" hadoop-mapreduce-client-core.jar; \
    get "org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar" hadoop-shaded-guava.jar; \
    get "org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar" htrace-core4.jar; \
    get "commons-configuration/commons-configuration/1.10/commons-configuration-1.10.jar" commons-configuration.jar; \
    get "org/apache/commons/commons-configuration2/2.10.1/commons-configuration2-2.10.1.jar" commons-configuration2.jar; \
    get "com/google/guava/guava/32.1.3-jre/guava-32.1.3-jre.jar" guava.jar; \
    get "com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar" jackson-databind.jar; \
    get "org/apache/commons/commons-lang3/3.14.0/commons-lang3-3.14.0.jar" commons-lang3.jar; \
    get "org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar" slf4j-api.jar; \
    get "com/fasterxml/woodstox/woodstox-core/6.5.1/woodstox-core-6.5.1.jar" woodstox-core.jar; \
    get "org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar" stax2-api.jar; \
    get "com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar" jackson-core.jar; \
    get "com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar" jackson-annotations.jar; \
    get "com/google/re2j/re2j/1.1/re2j-1.1.jar" re2j.jar; \
    get "org/apache/commons/commons-collections4/4.4/commons-collections4-4.4.jar" commons-collections4.jar; \
    get "org/apache/commons/commons-text/1.11.0/commons-text-1.11.0.jar" commons-text.jar; \
    get "commons-io/commons-io/2.15.1/commons-io-2.15.1.jar" commons-io.jar; \
    get "com/google/code/gson/gson/2.10.1/gson-2.10.1.jar" gson.jar; \
    get "org/apache/kerby/kerb-core/1.0.1/kerb-core-1.0.1.jar" kerb-core.jar; \
    get "org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar" hadoop-auth.jar; \
    get "org/apache/hadoop/hadoop-annotations/${HADOOP_VERSION}/hadoop-annotations-${HADOOP_VERSION}.jar" hadoop-annotations.jar; \
    get "com/jcraft/jsch/0.1.55/jsch-0.1.55.jar" jsch.jar; \
    get "org/apache/zookeeper/zookeeper/3.8.4/zookeeper-3.8.4.jar" zookeeper.jar; \
    get "commons-logging/commons-logging/1.3.0/commons-logging-1.3.0.jar" commons-logging.jar; \
    get "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" commons-pool2.jar; \
    get "org/apache/httpcomponents/httpclient/4.5.14/httpclient-4.5.14.jar" httpclient.jar; \
    get "org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar" httpcore.jar; \
    get "commons-codec/commons-codec/1.16.0/commons-codec-1.16.0.jar" commons-codec.jar; \
    get "com/google/protobuf/protobuf-java/3.21.12/protobuf-java-3.21.12.jar" protobuf-java.jar; \
    get "org/apache/kerby/kerb-common/1.0.1/kerb-common-1.0.1.jar" kerb-common.jar; \
    get "org/apache/kerby/kerb-util/1.0.1/kerb-util-1.0.1.jar" kerb-util.jar; \
    get "org/apache/kerby/kerb-crypto/1.0.1/kerb-crypto-1.0.1.jar" kerb-crypto.jar; \
    get "org/apache/kerby/kerby-config/1.0.1/kerby-config-1.0.1.jar" kerby-config.jar; \
    chown -R flink:flink /opt/flink

# The warehouse and checkpoint volumes mount as root-owned. Iceberg's hadoop
# catalog creates its metadata directory on first write and fails with
# "Mkdirs failed" when it cannot, which is the only symptom you get. Creating
# the mount points with the right owner in the image is what makes the first
# run work without a manual chown.
RUN mkdir -p /warehouse /checkpoints && chown -R flink:flink /warehouse /checkpoints

USER flink