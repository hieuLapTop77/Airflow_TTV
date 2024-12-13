import datetime
import os
import shutil
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from requests.auth import HTTPBasicAuth

# Variables
URL = "http://airflow-webserver:8080/api/v1/dags/"
AIRFLOW_USER = Variable.get("airflow_user")
AIRFLOW_PASSWORD = Variable.get("airflow_password")

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 0 */4 * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Logs", "Clear"],
    max_active_runs=1,
)
def ClearLogs():

    def run(url: str, del_date):
        list_data = []
        payload = {
            "offset": 0,
            "start_date_lte": f"{del_date}"
        }
        while payload["offset"] < 1000:
            skip = payload["offset"]
            print(f"Running on offset: {skip}")
            response = requests.get(url, auth=HTTPBasicAuth(
                AIRFLOW_USER, AIRFLOW_PASSWORD), params=payload, timeout=None)
            if response.status_code == 200:
                data = response.json()["dag_runs"]
                if len(data) < 1:
                    break
                list_data.extend(data)
                payload["offset"] += 100
            else:
                print("Error please check api")
                break
        return list_data

    @task
    def clear_log_files():
        list_dags_logs = ["Nikko_Clickup_Merge_Email_Purchase_v1", "Nikko_Clickup_Merge_Email_Sales_v1",
                          "Banhang_Clickup", "Clickup_Comment_Purchase", "Clickup_Comment_Sales",
                          'Aviatar_Accept_Aircraft', 'Aviatar_Add_Flight', 'Aviatar_Change_Logs', 'Aviatar_Close_Flight',
                          'Aviatar_CRS', 'Aviatar_Supplementary_Data',
                          'Clickup_Comment_khach_hang', 'Clickup_Space_Ban_hang'
                          ]
        today = datetime.now(timezone.utc) + timedelta(hours=7)
        yesterday = today - timedelta(hours=1)
        for dag in list_dags_logs:
            print(f"Deleting logs from {dag} older than yesterday")
            log_path = f"/opt/airflow/logs/{dag}"
            for root, dirs, files in os.walk(log_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_mod_time = datetime.fromtimestamp(
                        os.path.getmtime(file_path), tz=timezone.utc)
                    if file_mod_time < yesterday:
                        os.remove(file_path)
                        print(f"Deleted file: {file_path}")

                for dir in dirs:
                    dir_path = os.path.join(root, dir)
                    dir_mod_time = datetime.fromtimestamp(
                        os.path.getmtime(dir_path), tz=timezone.utc)
                    if dir_mod_time < yesterday:
                        shutil.rmtree(dir_path)
                        print(f"Deleted directory: {dir_path}")

    @task
    def clear_logs_airflow():
        list_dags_logs = ["Nikko_Clickup_Merge_Email_Purchase_v1", "Nikko_Clickup_Merge_Email_Sales_v1",
                          "Banhang_Clickup", "Clickup_Comment_Purchase", "Clickup_Comment_Sales",
                          'Aviatar_Accept_Aircraft', 'Aviatar_Add_Flight', 'Aviatar_Change_Logs', 'Aviatar_Close_Flight',
                          'Aviatar_CRS', 'Aviatar_Supplementary_Data',
                          'Clickup_Comment_khach_hang', 'Clickup_Space_Ban_hang'
                          ]
        data = []
        for i in list_dags_logs:
            url = URL + i + "/dagRuns"
            today = datetime.now(timezone.utc) + timedelta(hours=7)
            date_from = (today.date() - timedelta(hours=1)
                         ).strftime('%Y-%m-%d')
            result = run(url, date_from)
            data.extend(result)

        df = pd.DataFrame(data)
        for i in range(0, len(df)):
            url = URL + df["dag_id"][i] + "/dagRuns/" + df["dag_run_id"][i]
            response = requests.delete(url, auth=HTTPBasicAuth(
                AIRFLOW_USER, AIRFLOW_PASSWORD), timeout=None)
            if response.status_code == 204:
                print('Logs đã được xóa thành công.')
            else:
                print('Không thể xóa logs. Mã lỗi:', response.status_code)
                print('Chi tiết lỗi:', response.json())

    clear_logs_airflow() >> clear_log_files()


dag = ClearLogs()
