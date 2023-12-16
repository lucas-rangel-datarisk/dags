import great_expectations as gx

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable

URL = Variable.get('url')

@dag(dag_id='test_tools', start_date=days_ago(1), catchup=False, max_active_runs=1)
def taskflow():

	@task
	def say_hello():
		print('hello world')

	@task
	def test_gx():
		context = gx.get_context()
		context = context.convert_to_file_context()
		print(context)

	# say_hello() >> test_spark
	test_gx()

taskflow()
