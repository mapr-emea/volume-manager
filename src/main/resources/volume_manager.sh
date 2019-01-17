#/bin/bash

# Determine the desired action
ACTION=$1

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# Define the different directories and files
BASE_DIR=`dirname "$this"`/..
BIN_DIR=$BASE_DIR/bin
LIB_DIR=$BASE_DIR/lib
LOGS_DIR=$BASE_DIR/logs
CONF_DIR=$BASE_DIR/conf
PID_FILE=/tmp/volume-manager.pid

# Determine MapR home
if [ -z ${MAPR_HOME+x} ]; then
  export MAPR_HOME=/opt/mapr
fi

# Execute the desired action
case $ACTION in
	(start)
		# Build the Java class path
		CLASSPATH=$(echo "$LIB_DIR"/*.jar | tr ' ' ':')
        CLASSPATH=$CLASSPATH:/opt/mapr/lib/maprfs-6.1.0-mapr.jar

		# Try to locate the Java Runtime Environment
		if [ -z "$JAVA_HOME" ]; then
			if [ -f $MAPR_HOME/conf/env.sh ]; then
				. $MAPR_HOME/conf/env.sh
			fi
		fi

		if [ -z "$JAVA_HOME" ]; then
			echo "Unable to determine JAVA_HOME, exiting..."
			exit 1
		fi

		LOG_FILE=$LOGS_DIR/volume-manager.log
		OUT_FILE=$LOGS_DIR/volume-manager.out

		# Start the volume manager
		$JAVA_HOME/bin/java -Dlog.file="$LOG_FILE" -Dlog4j.configuration=file://"$CONF_DIR"/log4j.properties -Djavax.net.ssl.trustStore=$MAPR_HOME/conf/ssl_truststore -Xms128m -Xmx128m -Djava.library.path=/opt/mapr/hadoop/hadoop-2.7.0/lib/native -classpath "$CLASSPATH" volumes.VolumeManager --configDir "$CONF_DIR"  > "$OUT_FILE" 2>&1 < /dev/null &
		PID=$!
		echo $PID > $PID_FILE
		echo "Started Volume Manager with PID $PID"
	;;

	(stop)
		if [ -f "$PID_FILE" ]; then
			PID=`cat "$PID_FILE"`
			kill $PID > /dev/null 2>&1
			if [ $? -eq 0 ]; then
				rm -f $PID_FILE
				echo "Stopped Volume Manager with PID $PID"
				exit 0
			fi
			echo "Found Volume Manager with PID $PID, but could not stop it"
			exit 1
		fi
		echo "Unable to determine if Volume Manager is running"
		exit 1
	;;

	(status)
		if [ -f "$PID_FILE" ]; then
			PID=`cat "$PID_FILE"`
			ps -p $PID >/dev/null 2>&1
			if [ $? -eq 0 ] ; then
				echo "Volume Manager is running with PID $PID"
				exit 0
			fi
		fi
		echo "Volume Manager is not running"
		exit 1
	;;

	(*)
		echo "Please specify either start, stop, or status"
		exit 1
	;;
esac
