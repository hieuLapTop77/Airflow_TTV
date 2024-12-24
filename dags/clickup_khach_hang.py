import concurrent.futures
import copy
import json
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.helper import call_query_sql
from common.hook import hook
from common.variables import API_TOKEN, CLICKUP_CREATE_TASK

# Variables
LIST_ID_TASK_DESTINATION = 901802834969

CLICKUP_STATUS = "To Do"

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}
BODY_TEMPLATE = {
    "name": "",
    "text_content": "",
    "description": "",
    "status": None,
    "date_created": "",
    "date_updated": None,
    "date_closed": None,
    "date_done": None,
    "archived": False,
    "assignees": [],
    "group_assignees": [],
    "checklists": [],
    "tags": [],
    "parent": None,
    "priority": None,
    "due_date": None,
    "start_date": None,
    "points": None,
    "time_estimate": None,
    "time_spent": 0,
    "custom_fields": [
        {
            "id": "786ed003-7bad-4dbc-aa55-591f6959ee64",
            "name": "Created by"
        },
        {
            "id": "5cb6280d-6a9f-43fc-b67e-b4b185babae0",
            "name": "Mã khách hàng"
        },
        {
            "id": "d5759a1d-79b2-4554-a741-105b526372b1",
            "name": "Địa chỉ"
        }
    ],
    "attachments": [],
}


@dag(
    default_args=default_args,
    schedule_interval="10 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup", "khach hàng"],
    max_active_runs=1,
)
def Clickup_post_khach_hang():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }

    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['Ten_khach_hang']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = CLICKUP_STATUS
        # body["description"] = df_row["description"]
        for field in body["custom_fields"]:
            if field["id"] == "5cb6280d-6a9f-43fc-b67e-b4b185babae0":  # "Mã khách hàng"
                field["value"] = df_row['Ma_khach_hang'] if df_row['Ma_khach_hang'] else None
            elif field["id"] == "786ed003-7bad-4dbc-aa55-591f6959ee64":  # "created by"
                field["value"] = df_row['created_by'] if df_row['created_by'] else None
            elif field["id"] == "d5759a1d-79b2-4554-a741-105b526372b1":  # "địa chỉ"
                field["value"] = df_row['Dia_chi'] if df_row['Dia_chi'] else None
        return json.loads(json.dumps(body, ensure_ascii=False))

    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            LIST_ID_TASK_DESTINATION), json=main_task_payload, headers=headers, timeout=None)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            id_khach_hang = df_row['Ma_khach_hang']
            # Update status clickup
            query = f"""
                INSERT INTO [dbo].[log_clickup_khach_hang]
                    ([Ma_khach_hang]
                    ,[dtm_creation_date])
                VALUES(N'{id_khach_hang}',getdate())
            """
            call_query_sql(query=query)
            print(
                f"UPDATE: Update status clickup với mã khách hàng: {id_khach_hang} successfully")
        else:
            print("create task fail: ", res.status_code, "Error: ", res.json())

    @task
    def create_order_clickup():
        sql_conn = hook.get_conn()
        sql = """
            select a.account_object_code Ma_khach_hang
                ,a.account_object_name Ten_khach_hang
                ,case when len(a.address) > 3 then a.address else '' end  Dia_chi
                , a.created_by
            from [dbo].[3rd_misa_api_account_objects] a
            where is_customer = 'True' and CAST(a.account_object_code AS VARCHAR) not in 
            (select CAST(Ma_khach_hang AS VARCHAR) Ma_khach_hang from [dbo].[log_clickup_khach_hang]);

        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    create_order_clickup()


dag = Clickup_post_khach_hang()
