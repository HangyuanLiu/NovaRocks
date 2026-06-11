# Standalone Hive Metastore for the NovaRocks Iceberg test fixture.
# Adds hadoop-aws + the AWS SDK bundle so the metastore can resolve s3a://
# warehouse locations against MinIO. Backed by the image's embedded Derby DB
# (cleared on container restart — fine for tests).
FROM apache/hive:4.0.0

USER root

# IMPORTANT: HADOOP_VERSION must match the hadoop-*.jar already bundled in
# /opt/hive/lib. Verify with:
#   docker run --rm --entrypoint bash apache/hive:4.0.0 -lc 'ls /opt/hive/lib/hadoop-common-*.jar'
# Adjust the ARG below to that exact version before building.
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_VERSION=1.12.367

RUN set -eux; \
    cd /opt/hive/lib; \
    curl -fsSL -o "hadoop-aws-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

COPY core-site.xml /opt/hive/conf/core-site.xml

USER hive
