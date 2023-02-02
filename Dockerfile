FROM bitnami/spark:latest
RUN pip install findspark
USER root
RUN apt-get update
RUN apt-get -y install curl
RUN curl https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.7.2/mongodb-driver-core-4.7.2.jar --output /opt/bitnami/spark/jars/mongodb-driver-core-4.7.2.jar
RUN curl https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-reactivestreams/4.3.2/mongodb-driver-reactivestreams-4.3.2.jar --output /opt/bitnami/spark/jars/mongodb-driver-reactivestreams-4.3.2.jar
RUN curl https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector/10.0.5/mongo-spark-connector-10.0.5.jar --output /opt/bitnami/spark/jars/mongo-spark-connector-10.0.5.jar
RUN curl https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.8.1/mongodb-driver-3.8.1.jar --output /opt/bitnami/spark/jars/mongodb-driver-3.8.1.jar 
RUN curl https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.11/mongo-java-driver-3.12.11.jar --output /opt/bitnami/spark/jars/mongo-java-driver-3.12.11.jar
