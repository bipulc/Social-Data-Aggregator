#/bin/ksh

# Wrapper script for dataCollection.py for running as cron job

# Set Environment Variables

SCRIPT_HOME=/home/centos/sentiment_analysis/bin
CONFIG_FILE=/home/centos/sentiment_analysis/etc/configfile
CONFLUENT_CURRENT=/kafka/data
DATA_SOURCE=twitter
SCRIPT_NAME=dataCollection.py
PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/centos/.local/bin:/home/centos/bin
PATH=$PATH:/bdsw/hadoop-3.0.0/bin:/bdsw/confluent-3.3.1/bin:/home/centos/sentiment_analysis/bin

export SCRIPT_HOME CONFIG_FILE CONFLUENT_CURRENT SCRIPT_NAME PATH

# Get PID to check if the process if already running and start only if it's not running

PID=`ps -ef| grep $SCRIPT_HOME/$SCRIPT_NAME | grep -v grep | awk '{print $2}'`

if [ -z ${PID} ]; then
    $SCRIPT_HOME/$SCRIPT_NAME -c $CONFIG_FILE -s $DATA_SOURCE
else
    echo "$SCRIPT_HOME/$SCRIPT_NAME already running. PID = $PID"
fi
