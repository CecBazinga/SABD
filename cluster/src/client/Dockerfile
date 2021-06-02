# Creates client for interfacing with Spark & hadoop
#
#

FROM ubuntu
USER root
ARG DEBIAN_FRONTEND=noninteractive

# install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo openssh-server rsync default-jdk vim net-tools iputils-ping wget git
RUN apt install -y  maven openjdk-8-jdk openjdk-8-jre

# # java
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64/jre
ENV PATH $PATH:$JAVA_HOME/bin


RUN wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz ; tar -zxf spark-3.1.1-bin-hadoop3.2.tgz -C /usr/local/ ; rm spark-3.1.1-bin-hadoop3.2.tgz

RUN cd /usr/local && ln -s ./spark-3.1.1-bin-hadoop3.2 spark

RUN cd /

RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz ; tar -zxf hadoop-3.2.2.tar.gz -C /usr/local/ ; rm hadoop-3.2.2.tar.gz
RUN cd /usr/local && ln -s ./hadoop-3.2.2 hadoop


ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
ENV HDFS_NAMENODE_USER "root"
ENV HDFS_DATANODE_USER "root"
ENV HDFS_SECONDARYNAMENODE_USER "root"
ENV YARN_RESOURCEMANAGER_USER "root"
ENV YARN_NODEMANAGER_USER "root"

ADD config/core-site.xml $SPARK_HOME/conf/core-site.xml
ADD config/hdfs-site.xml $SPARK_HOME/conf/hdfs-site.xml
ADD config/yarn-site.xml $SPARK_HOME/conf/yarn-site.xml
ADD SparkClient /usr/local/SparkClient

RUN cd /usr/local/SparkClient && mvn clean compile assembly:single
RUN cd /usr/local && mv SparkClient/target/QueryExecutor-1-jar-with-dependencies.jar ./sparkapp.jar


ADD config/bootstrap.sh /usr/local/bootstrap.sh
RUN chown root:root /usr/local/bootstrap.sh
RUN chmod 700 /usr/local/bootstrap.sh
# 
ENV BOOTSTRAP /usr/local/bootstrap.sh
# 
CMD /usr/local/bootstrap.sh


EXPOSE 8080 7077 4040
