# need compatible both JAVA_HOME and SPARK_HOME
#
# i.e. not setting SPARK_HOME, when getOrCreating SparkSession, causes a very hard to debug:
#	java.lang.IllegalArgumentException: requirement failed: Can only call getServletHandlers on a running MetricsSystem
# 
. ../../spark-standalone/env.sh

main=~/DataEng2022/de-capstone/src/main
export PYTHONPATH=$(pwd):$(pwd)/plugins:$main:$main/plugins:$PYTHONPATH

