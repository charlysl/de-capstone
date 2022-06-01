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