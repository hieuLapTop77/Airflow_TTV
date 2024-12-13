import json

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.variables import MISA_ACCESS_CODE, MISA_API_GET_TOKEN, MISA_APP_ID

MISA_ACCESS_CODE = 'tHnqbwuUvJzrHs/PYPSnPuIUphxlsml51ovMMB79Q7Y3lvV7BqQzdzq1SinDohXcNJkshfRzz/srRqEKEpHFv6Uamz5iB4QkDvQT/xRzTbWIHuYAX8Blsqi8ne6vcd/8k8yd/wpLgpwTGiyvuZW1ZsjL7s5vytnoow5XvD9jyG9/FLMScUGNIcFZctuYQ5IvWU/gn6wAnnLmELMD3br6mg55yBBeCmyn38hrfvzZ7a/gqgvZzOEkpixLGmYrDm3GykIJgt56dP2rXt16rj+CQg=='
MISA_APP_ID = '9a03dccd-92e8-4e85-b5bf-0f98401bf5d8'

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "Token"],
    max_active_runs=1,
)
def Misa_Generate_Token():
    ######################################### API ################################################
    @task
    def call_api_generate_token():
        body = {
            "app_id": f"{MISA_APP_ID}",
            "access_code": f"{MISA_ACCESS_CODE}",
            "org_company_code": "actapp"
        }
        response = requests.post(MISA_API_GET_TOKEN, json=body, timeout=None)
        if response.status_code == 200:
            data = json.loads(response.json().get("Data"))
            Variable.set("misa_token", data.get('access_token'))
            print("Setting variable successfully")
        else:
            print("Error please check api with status ", response.status_code)
    ######################################### INSERT DATA ###############################################

    ############ DAG FLOW ############
    call_api_generate_token()


dag = Misa_Generate_Token()
