from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

URL = Variable.get('url')

@dag(dag_id='test_dag', start_date=days_ago(1), catchup=False)
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
    def show_urls(url):
	print(url)

    @task
    def say_hello():
	print('hello world')

    show_urls.partial().expand(url=get_urls()) >> say_hello()

taskflow()
