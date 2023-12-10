from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

URL = Variable.get('url')

@dag(dag_id='test_tools', start_date=days_ago(1), catchup=False, max_active_runs=1)
def taskflow():

	@task
	def say_hello():
		print('hello world')

	test_spark = SparkSubmitOperator(
		task_id='test_spark',
		application='/incluce/pyspark_script.py',
		conn_id='spark_master'
	)

	say_hello() >> test_spark

taskflow()
