#/bin/ksh

# Wrapper script for flume-ng for running as cron job to collect data from Kafka topic and write to HDFS sink

# Set Environment Variables

SCRIPT_HOME=/bdsw/apache-flume-1.8.0-bin/bin/
CONFIG_FILE=/home/centos/sentiment_analysis/etc/hdfs-sink.conf
SCRIPT_NAME=flume-ng
AGENT_NAME=hdfsagent1
FLUME_HOME=/bdsw/apache-flume-1.8.0-bin

PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/centos/.local/bin:/home/centos/bin
PATH=$PATH:/bdsw/hadoop-3.0.0/bin:/bdsw/confluent-3.3.1/bin:/home/centos/sentiment_analysis/bin
PATH=$PATH:$FLUME_HOME/bin

export SCRIPT_HOME CONFIG_FILE SCRIPT_NAME FLUME_HOME PATH AGENT_NAME

PID=`ps -ef| grep flume | grep $AGENT_NAME | grep -v grep | awk '{print $2}'`

if [ -z ${PID} ]; then
    $SCRIPT_HOME/$SCRIPT_NAME agent --conf-file $CONFIG_FILE --name $AGENT_NAME --conf $FLUME_HOME/conf
else
    echo "$SCRIPT_HOME/$SCRIPT_NAME already running. PID = $PID"
fi