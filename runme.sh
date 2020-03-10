#!/bin/env bash

gradle build

QUESTION=monolithic
JAR_FILE=build/libs/cs455-assignment3.jar
INPUT_DIRS="/data/metadata /data/analysis"
OUT_DIR=/home/$QUESTION-out
LOG_FILE=$QUESTION.log
PART_R_FILE=$OUT_DIR/part-r-00000
RESULT_FILE=$QUESTION.results

$HADOOP_HOME/bin/hdfs dfs -rm -r $OUT_DIR
$HADOOP_HOME/bin/hadoop jar $JAR_FILE cs455.hadoop.$QUESTION.Job $INPUT_DIRS $OUT_DIR 2>&1 | tee $LOG_FILE
$HADOOP_HOME/bin/hdfs dfs -cat $PART_R_FILE > $RESULT_FILE

QUESTION=seven
JAR_FILE=build/libs/cs455-assignment3.jar
INPUT_DIRS="/data/analysis"
OUT_DIR=/home/$QUESTION-out
LOG_FILE=$QUESTION.log
PART_R_FILE=$OUT_DIR/part-r-00000
RESULT_FILE=$QUESTION.results

$HADOOP_HOME/bin/hdfs dfs -rm -r $OUT_DIR
$HADOOP_HOME/bin/hadoop jar $JAR_FILE cs455.hadoop.$QUESTION.Job $INPUT_DIRS $OUT_DIR 2>&1 | tee $LOG_FILE
$HADOOP_HOME/bin/hdfs dfs -cat $PART_R_FILE > $RESULT_FILE

QUESTION=nine
JAR_FILE=build/libs/cs455-assignment3.jar
INPUT_DIRS="/data/metadata /data/analysis"
OUT_DIR=/home/$QUESTION-out
LOG_FILE=$QUESTION.log
PART_R_FILE=$OUT_DIR/part-r-00000
RESULT_FILE=$QUESTION.results

$HADOOP_HOME/bin/hdfs dfs -rm -r $OUT_DIR
$HADOOP_HOME/bin/hadoop jar $JAR_FILE cs455.hadoop.$QUESTION.Job $INPUT_DIRS $OUT_DIR 2>&1 | tee $LOG_FILE
$HADOOP_HOME/bin/hdfs dfs -cat $PART_R_FILE > $RESULT_FILE

QUESTION=ten
JAR_FILE=build/libs/cs455-assignment3.jar
INPUT_DIRS="/data/metadata /data/analysis"
OUT_DIR=/home/$QUESTION-out
LOG_FILE=$QUESTION.log
PART_R_FILE=$OUT_DIR/part-r-00000
RESULT_FILE=$QUESTION.results

$HADOOP_HOME/bin/hdfs dfs -rm -r $OUT_DIR
$HADOOP_HOME/bin/hadoop jar $JAR_FILE cs455.hadoop.$QUESTION.Job $INPUT_DIRS $OUT_DIR 2>&1 | tee $LOG_FILE
$HADOOP_HOME/bin/hdfs dfs -cat $PART_R_FILE > $RESULT_FILE
