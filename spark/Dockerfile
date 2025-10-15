FROM docker.io/bitnamilegacy/spark:3.5

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl && apt-get clean

# Install Python dependencies
RUN pip install --no-cache-dir boto3 pandas pyarrow delta-spark==3.2.0
RUN rm -f /opt/bitnami/spark/jars/mysql-connector-java-8.0.33.jar
RUN wget -O /opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar


# Add matching Delta + AWS JARs
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.25.32/s3-2.25.32.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.699/aws-java-sdk-bundle-1.12.699.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/3.2.0/delta-core_2.12-3.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar \
    && mv *.jar /opt/bitnami/spark/jars/
    

