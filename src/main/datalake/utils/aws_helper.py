import os
import configparser

class AwsHelper:

    @staticmethod
    def get_credentials_filepath():
        home = os.environ['HOME']
        filepath = f'{home}/.aws/credentials'
        return filepath

    @staticmethod
    def get_credentials():
        filepath = AwsHelper.get_credentials_filepath()
        config = configparser.ConfigParser()
        config.read(filepath)
        return config['default']

    @staticmethod
    def access_key():
        return AwsHelper.get_credentials().get('aws_access_key_id')

    @staticmethod
    def secret_key():
        return AwsHelper.get_credentials().get('aws_secret_access_key')

    @staticmethod
    def is_running_in_emr():
        """
        Description: detect if spark job is to be ran/running in an EMR cluster

        Return:
        - If invoked outside a Spark job (i.e. from Airflow), True if
          env var DE_CAPSTONE_IS_EMR is set.
        _ If invoked inside a Spark job, True is env var USER == hadoop
        """

        is_emr_key = 'DE_CAPSTONE_IS_EMR'
        if is_emr_key in os.environ:
            return os.environ[is_emr_key].lower() == 'true'

        #There are many other environment variables that could have 
        # been checked instead.
        key = 'USER'
        return key in os.environ and os.environ[key] == 'hadoop'

