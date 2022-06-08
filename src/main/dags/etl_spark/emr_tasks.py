from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.utils.task_group import TaskGroup


class EmrTasks():
    """
    Based on:
    https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr.html

    First example that worked:
    https://docs.aws.amazon.com/mwaa/latest/userguide/samples-emr.html

    Very useful for troubleshooting:
    https://aws.amazon.com/premiumsupport/knowledge-center/emr-spark-failed-step/
    """

    @staticmethod
    def start_emr_cluster(datalake_root):

        # --step-concurrency-level 10
        concurrency = 10
        # --log-uri 's3n://aws-logs-129584338512-us-east-1/elasticmapreduce/'
        logs = f"s3n{datalake_root[3:]}/emr_logs/"

        JOB_FLOW_OVERRIDES = {
            'Name': 'de-capstone-etl-cluster',
            'ReleaseLabel': 'emr-5.30.1',
            'Applications': [
                {
                    'Name': 'Spark'
                },
            ],
            'StepConcurrencyLevel': concurrency,
            'LogUri': logs,
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                ],
                #'KeepJobFlowAliveWhenNoSteps': False,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': 'spark-cluster',
            },
            'VisibleToAllUsers': True,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole'
        }

        return EmrCreateJobFlowOperator(
            task_id=EmrTasks._get_job_flow_id(),
            job_flow_overrides=JOB_FLOW_OVERRIDES
        )


    @staticmethod
    def add_emr_step(name, task_id, cmd):
        """
        Return: an Airflow task
        """

        aws_conn_id = 'aws_default'

        SPARK_STEPS = [
            {
                'Name': name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    #'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
                    'Args': cmd
                },
            }
        ]

        group_id = f"{task_id}_emr_group"

        with TaskGroup(
            group_id=group_id, prefix_group_id=False
        ) as emr_task_group:
            step_adder = EmrAddStepsOperator(
                task_id=task_id,
                job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{EmrTasks._get_job_flow_id()}', key='return_value') }}}}",
                aws_conn_id=aws_conn_id,
                steps=SPARK_STEPS,
            )
            step_checker = EmrStepSensor(
                task_id=f"{task_id}_watch_step",
                job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{EmrTasks._get_job_flow_id()}', key='return_value') }}}}",
                # see: https://stackoverflow.com/questions/64493332/jinja-templating-in-airflow-along-with-formatted-text
                step_id=f"{{{{ task_instance.xcom_pull(task_ids='{task_id}', key='return_value')[0] }}}}",
                aws_conn_id=aws_conn_id,
            )

            step_adder >> step_checker

        return emr_task_group

    @staticmethod
    def stop_emr_cluster():
        return EmrTerminateJobFlowOperator(
            task_id='stop_emr_cluster',
            job_flow_id=f"{{{{ task_instance.xcom_pull(task_ids='{EmrTasks._get_job_flow_id()}', key='return_value') }}}}"
        )

    @staticmethod
    def _get_job_flow_id():
        return 'start_emr_cluster'



"""
Cluster that works:

aws emr create-cluster --applications Name=Spark --tags 'app=analytics' 'environment=development' --ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-022972602af74d981","EmrManagedSlaveSecurityGroup":"sg-0923f20d75954812a","EmrManagedMasterSecurityGroup":"sg-0f4f89cd0b3a7169b"}' --step-concurrency-level 10 --release-label emr-5.30.1 --log-uri 's3n://my-emr-log-bucket/default_job_flow_location/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--conf","spark.jars.repositories=https://repos.spark-packages.org/","--conf","spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0","--conf","spark.yarn.appMasterEnv.DATALAKE_ROOT=s3a://de-capstone-2022/datalake","--conf","spark.yarn.appMasterEnv.STAGING_ROOT=hdfs:///de-capstone-etl-staging","--py-files","s3a://de-capstone-2022/datalake/etl_spark_jobs/datalake.zip","s3a://de-capstone-2022/datalake/etl_spark_jobs/etl_init_time_dim.py","2015-01-01"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"","Properties":"","Name":"Custom JAR"},{"Args":["spark-submit","--deploy-mode","cluster","--conf","spark.jars.repositories=https://repos.spark-packages.org/","--conf","spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0","--conf","spark.yarn.appMasterEnv.DATALAKE_ROOT=s3a://de-capstone-2022/datalake","--conf","spark.yarn.appMasterEnv.STAGING_ROOT=hdfs:///de-capstone-etl-staging","--py-files","s3a://de-capstone-2022/datalake/etl_spark_jobs/datalake.zip","s3a://de-capstone-2022/datalake/etl_spark_jobs/etl_init_time_dim.py","2015-01-01"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Custom JAR"}]' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master nodes"}]' --ebs-root-volume-size 10 --service-role EMR_DefaultRole --name 'de-capstone-etl-cluster' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1

--steps '[{"Args":["spark-submit","--deploy-mode","cluster","--conf","spark.jars.repositories=https://repos.spark-packages.org/","--conf","spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0","--conf","spark.yarn.appMasterEnv.DATALAKE_ROOT=s3a://de-capstone-2022/datalake","--conf","spark.yarn.appMasterEnv.STAGING_ROOT=hdfs:///de-capstone-etl-staging","--py-files","s3a://de-capstone-2022/datalake/etl_spark_jobs/datalake.zip","s3a://de-capstone-2022/datalake/etl_spark_jobs/etl_init_time_dim.py","2015-01-01"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"","Properties":"","Name":"Custom JAR"},{"Args":["spark-submit","--deploy-mode","cluster","--conf","spark.jars.repositories=https://repos.spark-packages.org/","--conf","spark.jars.packages=saurfang:spark-sas7bdat:3.0.0-s_2.12,org.apache.hadoop:hadoop-aws:3.2.0","--conf","spark.yarn.appMasterEnv.DATALAKE_ROOT=s3a://de-capstone-2022/datalake","--conf","spark.yarn.appMasterEnv.STAGING_ROOT=hdfs:///de-capstone-etl-staging","--py-files","s3a://de-capstone-2022/datalake/etl_spark_jobs/datalake.zip","s3a://de-capstone-2022/datalake/etl_spark_jobs/etl_init_time_dim.py","2015-01-01"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Custom JAR"}]'

aws emr create-cluster 
--applications Name=Spark 
--ec2-attributes 
'{"KeyName":"spark-cluster",
"InstanceProfile":"EMR_EC2_DefaultRole",
"SubnetId":"subnet-022972602af74d981",
"EmrManagedSlaveSecurityGroup":"sg-0923f20d75954812a",
"EmrManagedMasterSecurityGroup":"sg-0f4f89cd0b3a7169b"}' 
--step-concurrency-level 10 
--release-label emr-5.30.1 
--log-uri 's3n://my-emr-log-bucket/default_job_flow_location/' 
 --instance-groups 
 '[{"InstanceCount":1,
 "EbsConfiguration":{"EbsBlockDeviceConfigs":
 [{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},
 "VolumesPerInstance":2}]},
 "InstanceGroupType":"MASTER",
 "InstanceType":"m5.xlarge","Name":"Master nodes"}]' 
 --ebs-root-volume-size 10 
 --service-role EMR_DefaultRole 
 --name 'de-capstone-etl-cluster' 
 --scale-down-behavior TERMINATE_AT_TASK_COMPLETION 
 --region us-east-1
"""