. ./env.sh

$SPARK_HOME/sbin/start-master.sh -h 127.0.0.1 -p 7077 && $SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077

