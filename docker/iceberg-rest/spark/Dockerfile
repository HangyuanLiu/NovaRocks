ARG SPARK_BASE
FROM ${SPARK_BASE}

ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_SPARK_RUNTIME=iceberg-spark-runtime-3.5_2.12
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
USER root

COPY artifacts/iceberg-spark-runtime-3.5_2.12-1.11.0.jar /opt/spark/jars/
COPY artifacts/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/
COPY artifacts/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY artifacts/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN chmod 0644 \
    /opt/spark/jars/${ICEBERG_SPARK_RUNTIME}-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
