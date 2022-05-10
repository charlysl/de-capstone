from airflow.operators.bash import BashOperator

conf = {
  #'java': '/Users/charly/Library/Caches/Coursier/arc/https/github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11%252B28/OpenJDK11-jdk_x64_mac_hotspot_11_28.tar.gz/jdk-11+28/Contents/Home',
  'java': '/usr/local/opt/openjdk@8',
  'home': '/usr/local/opt/apache-spark/libexec',
  'host': '127.0.0.1',
  'port': 7077
}

env={
  'JAVA_HOME': conf['java'],
  'SPARK_HOME': conf['home']
  }

bin=f"{conf['home']}/sbin"
master_url = f"spark://{conf['host']}:{conf['port']}"

args = {
  'master': f"-h {conf['host']} -p {conf['port']}",
  'worker': f"spark://{conf['host']}:{conf['port']}"
}

cmd = {
  'start_master': f"{bin}/start-master.sh {args['master']}",
  'start_worker': f"{bin}/start-worker.sh {args['worker']}",
  'stop_master': f"{bin}/stop-master.sh {args['master']}",
  'stop_worker': f"{bin}/stop-worker.sh {args['worker']}",
}


def start_spark(dag):
  return BashOperator(
    task_id='start_spark',
    env=env,
    bash_command=f"{cmd['start_master']} && {cmd['start_worker']}",
    dag=dag
  )


def stop_spark(dag):
  return BashOperator(
    task_id='stop_spark',
    env=env,
    bash_command=f"{cmd['stop_worker']} && {cmd['stop_master']}",
    dag=dag
  )
