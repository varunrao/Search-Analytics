# HBase Indexer MapReduce

MapReduce batch job driver that takes input data from an HBase table and creates 
Solr index shards and writes the indexes into HDFS, in a flexible, scalable, and 
fault-tolerant manner. It also supports merging the output shards into a set of 
live customer-facing Solr servers in SolrCloud. 