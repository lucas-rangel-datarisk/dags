from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.models import Variable

URL = Variable.get('url')

@dag(dag_id='test_tools', start_date=days_ago(1), catchup=False, max_active_runs=1)
def taskflow():

	@task
	def get_urls():
		urls = []
		for month in range(1, 13):
			name = f'yellow_tripdata_2022-{month:02d}'
			data_url = f'{URL}/{name}.parquet.gz'
			urls.append(data_url)
		return urls

	@task
	def test_spark(urls):
		spark = SparkSession.Builder.appName('Testing PySpark').getOrCreate()
		sample_data = [{'name': 'John D.', 'age': 30}, {'name': 'Alice G.', 'age': 25},
				{'name': 'Bob T.', 'age': 35}, {'name': 'Eve A', 'age': 28}]
		df = spark.createDataFrame(sample_data)
		df_transformed = df.withColumn('name', regexp_replace(col('name'), '\\s+', ' '))
		df_transformed.show()
		print(df_transformed.show())

	@task
	def say_hello():
		print('hello world')

	show_urls(urls=get_urls()) >> say_hello()

taskflow()
