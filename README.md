# cs455-assignment3

Using MapReduce To Analyze The Million Song Dataset

## To-Do
* fix code for timbre and pitch, produce a flattened array for q7
* make sure loudness is being calculated correctly. higher number is loudest.
* go through reducer and ingore zero values where applicable

## HDFS
first time configuration:
$HADOOP_HOME/bin/hdfs namenode -format

start hdfs:
./start-dfs.sh

web portal:
http://phoenix:54301/dfshealth.html#tab-overview

stop hdfs:
./stop-dfs.sh

try-again:
rm -rf /s/phoenix/a/nobackup/cs455/sgaxcell/dfs
for host in $(cat conf/workers); do echo $host; ssh $host "rm -rf /s/$host/a/nobackup/cs455/sgaxcell/dfs"; done

## Yarn
start yarn:
./start-yarn.sh

web portal:
http://phoenix:54307/cluster

stop yarn:
./stop-yarn.sh

trust, but verify:
$HADOOP_HOME/bin/hdfs dfsadmin -report

## MapReduce Client
export HADOOP_CONF_DIR=${HOME}/cs455-assignment3/client-config

e.g.
$HADOOP_HOME/bin/hadoop jar msd-analysis.jar cs455.hadoop.msd.AnalysisJob /data/analysis /home/analysis/output
