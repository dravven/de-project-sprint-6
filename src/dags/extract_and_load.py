from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
from airflow.decorators import dag, task
import boto3

from vertica_python import connect



def fetch_s3_file(bucket: str, key: str):
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3_client.download_file(
        'sprint6',
        key,
        key
    )


def loads(str1):
    conn_info = connect(
        host='51.250.75.20',
        port=5433,
        user='DUROVVERYANDEXRU',
        password='VqrwunpqcT5kroi'
    )
    cur = conn_info.cursor()
    with open(f'{str1}.csv', "rb") as fs:
        my_file = fs.read().decode('utf-8', 'ignore')
        cur.copy(f"COPY DUROVVERYANDEXRU__STAGING.{str1} FROM STDIN NULL AS 'null' delimiter ','", my_file)
        conn_info.commit()

    cur.close()
    conn_info.close()


@dag(
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'example'],
    is_paused_upon_creation=False
)
def dag():

    BashOperator(
        task_id='also_run_this',
        bash_command='ls',
    )

    @task()
    def fetch_file():
        fetch_s3_file('data-bucket', 'group_log.csv')

    @task()
    def load_group_log():
        loads('group_log')

    fetch_file = fetch_file()
    load_group_log = load_group_log()
    fetch_file >> load_group_log # type: ignore



dag = dag()
