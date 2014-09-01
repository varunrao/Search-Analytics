#!/bin/bash
cd /usr/local/
mkdir apache-maven
cd apache-maven
wget http://mirror.olnevhost.net/pub/apache/maven/binaries/apache-maven-3.2.1-bin.tar.gz
tar xvf apache-maven-3.2.1-bin.tar.gz
export M2_HOME=/usr/local/apache-maven/apache-maven-3.2.1
export M2=$M2_HOME/bin
export PATH=$M2:$PATH
# Install app code
cd ~
git clone https://github.com/varunrao/Search-Analytics.git
cd Search-Analytics
## Install flume hbase serializer
cd hbase-serializer
mvn clean package
cp target/hbase-serializer-0.0.1-SNAPSHOT.jar /var/lib/flume/lib/
## Install flume custom Twitter4j Source
cd flume-sources
## Replace twitter variables
sed -i "s/consumer_Key/$consumer_Key/" flume.conf
sed -i "s/consumer_Secret/$consumer_Secret/" flume.conf
sed -i "s/access_Token/$access_Token/" flume.conf
sed -i "s/access_TokenSecret/$access_TokenSecret/" flume.conf
mvn clean package
cp flume.conf /etc/flume/conf/

## Install hbase solr indexer
cd hbase-indexer
mvn clean package -DskipTests=true
bin/hbase-indexer server