#!/bin/bash

SPARK_HOME=/opt/spark-2.1.1-bin-hadoop2.7/
INADCO_CSJB_HOME=/opt/inadco-csjb-dist-2017-07-04/dist/
SPARK_MASTER_HOST=10.189.168.221
app_name=InadcoHiveThriftServer
csjb_pid=$INADCO_CSJB_HOME/inadco-csjb-server.pid
JAR_FILE=inadco-csjb-assembly-2.1.1.jar


#Chk to see if app is running

function is_running {
  if [ ! -z "$csjb_pid" ]; then
    if [ -f "$csjb_pid" ]; then
      if [ -s "$csjb_pid" ]; then
        echo "Existing PID file found during start."
        if [ -r "$csjb_pid" ]; then
          PID=`cat "$csjb_pid"`
          ps -p $PID >/dev/null 2>&1
          if [ $? -eq 0 ] ; then
            echo "$app_name appears to still be running with PID $PID. Start aborted."
            exit 1
          else
            echo "Removing/clearing stale PID file."
            rm -f "$csjb_pid" >/dev/null 2>&1
            if [ $? != 0 ]; then
              if [ -w "$csjb_pid" ]; then
                cat /dev/null > "$csjb_pid"
              else
                echo "Unable to remove or clear stale PID file. Start aborted."
                exit 1
              fi
            fi
          fi
        else
          echo "Unable to read PID file. Start aborted."
          exit 1
        fi
      else
        rm -f "$csjb_pid" >/dev/null 2>&1
        if [ $? != 0 ]; then
          if [ ! -w "$csjb_pid" ]; then
            echo "Unable to remove or write to empty PID file. Start aborted."
            exit 1
          fi
        fi
      fi
    fi
  fi

}

#Chk to see if app is running
is_running


echo "Starting $app_name"

if [[ -z "$SPARK_HOME" ]]; then
        echo "SPARK_HOME is not set.";
        exit 1;
fi

if [[ -z "$INADCO_CSJB_HOME" ]]; then
        echo "$INADCO_CSJB_HOME is not set.";
        exit 1;
fi

mkdir -p $INADCO_CSJB_HOME/log
if [[ -d $INADCO_CSJB_HOME/log ]]; then
                echo "Creating log dir $INADCO_CSJB_HOME/log";

fi

$SPARK_HOME/bin/spark-submit --driver-memory 4g --class com.inadco.cassandra.spark.jdbc.InadcoCSJServer --master spark://$SPARK_MASTER_HOST:7077 $INADCO_CSJB_HOME/$JAR_FILE >>$INADCO_CSJB_HOME/log/log.out \
2>>$INADCO_CSJB_HOME/log/log.err & echo $! > $csjb_pid
