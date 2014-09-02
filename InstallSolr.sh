#!/bin/bash
export JAVA_HOME=/usr/jdk64/jdk1.6.0_31/ 
export PATH=$JAVA_HOME/bin:$PATH
yum install -y ant
cd ~
wget http://archive.apache.org/dist/lucene/solr/4.7.2/solr-4.7.2.tgz
tar -xvf solr-4.7.2.tgz
cd ~
git clone https://github.com/LucidWorks/banana.git
cd banana
sed -i 's/localhost/'"`hostname -i`"'/' src/config.js
sed -i 's/localhost/'"`hostname -i`"'/' src/app/dashboards/default.json
rm -rf build
mkdir build
ant
 cp build/banana*.war ~/solr-4.7.2/example/webapps/banana.war
cp jetty-contexts/banana-context.xml ~/solr-4.7.2/example/contexts/
cd ~/solr-4.7.2/example/
 java -jar start.jar &