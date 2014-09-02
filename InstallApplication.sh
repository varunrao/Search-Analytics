#!/bin/bash
consumer_Key=
consumer_Secret=
access_Token=
access_TokenSecret=
cd ~
mkdir apache-maven
cd apache-maven
wget http://mirror.olnevhost.net/pub/apache/maven/binaries/apache-maven-3.2.1-bin.tar.gz
tar xvf apache-maven-3.2.1-bin.tar.gz
export M2_HOME=~/apache-maven/apache-maven-3.2.1
export M2=$M2_HOME/bin
export PATH=$M2:$PATH
# Install app code
cd ~/Search-Analytics
## Install flume hbase serializer
cd ~/Search-Analytics/hbase-serializer
mvn clean package
cp target/hbase-serializer-0.0.1-SNAPSHOT.jar /usr/lib/flume/lib/
## Install flume custom Twitter4j Source
cd ~/Search-Analytics/flume-sources
## Replace twitter variables
sed -i "s/consumer_Key/$consumer_Key/" flume.conf
sed -i "s/consumer_Secret/$consumer_Secret/" flume.conf
sed -i "s/access_Token/$access_Token/" flume.conf
sed -i "s/access_TokenSecret/$access_TokenSecret/" flume.conf
mvn clean package
cp flume.conf /etc/flume/conf/
cp target/flume-sources-1.0-SNAPSHOT.jar /usr/lib/flume/lib/

## Install hbase solr indexer
cd ~/Search-Analytics/hbase-indexer
mvn package -DskipTests=true
bin/hbase-indexer server