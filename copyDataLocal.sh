#!/bin/env bash

#$HADOOP_HOME/bin/hdfs dfs -mkdir /cs455
#$HADOOP_HOME/bin/hdfs dfs -mkdir /cs455/data
#$HADOOP_HOME/bin/hdfs dfs -mkdir /cs455/data/analysis
#$HADOOP_HOME/bin/hdfs dfs -mkdir /cs455/data/metadata

$HADOOP_HOME/bin/hdfs dfs -cp /data/analysis/analysis1.csv /home/cs455/data/analysis
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata1.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata2.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata3.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata4.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata5.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata6.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata7.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata8.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata9.csv /home/cs455/data/metadata
$HADOOP_HOME/bin/hdfs dfs -cp /data/metadata/metadata10.csv /home/cs455/data/metadata

