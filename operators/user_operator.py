
import datetime as dt
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpToGcsOperator(BaseOperator):


    @apply_defaults
    def __init__(self, default_args):
        super(HttpToGcsOperator, self).__init__( default_args=default_args)

    def execute(self, context):
        print("abc")