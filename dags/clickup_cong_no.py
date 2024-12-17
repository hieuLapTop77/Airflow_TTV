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
LIST_ID_TASK_DESTINATION = 901802888473
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
            "id": "59c40419-693a-446b-bce4-89f24ff9c2fe",
            "name": "Loại công nợ"
        },
        {
            "id": "921e74d6-cc36-4e09-934f-a4d6089032b2",
            "name": "Mã account"
        },
        {
            "id": "f3b33d34-80c4-4385-9e49-70c8c56a0cdd",
            "name": "Số tiền công nợ"
        },
        {
            "id": "1a996ed2-5ad0-4678-8c0f-ae2a0182d6ae",
            "name": "số tiền công nợ hóa đơn"
        }
    ],
    "attachments": [],
}


@dag(
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup", "công nợ"],
    max_active_runs=1,
)
def Clickup_post_cong_no():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }

    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['name']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        body['status'] = CLICKUP_STATUS
        # body["description"] = df_row["description"]
        for field in body["custom_fields"]:
            if field["id"] == "921e74d6-cc36-4e09-934f-a4d6089032b2":  # "Mã"
                field["value"] = df_row['Ma'] if df_row['Ma'] else None
            elif field["id"] == "f3b33d34-80c4-4385-9e49-70c8c56a0cdd":  # "số tiền công nợ"
                field["value"] = df_row['so_tien_cong_no'] if df_row['so_tien_cong_no'] else None
            elif field["id"] == "1a996ed2-5ad0-4678-8c0f-ae2a0182d6ae":  # "số tiền công nợ hóa đơn"
                field["value"] = df_row['so_tien_cong_no_hd'] if df_row['so_tien_cong_no_hd'] else None
            elif field["id"] == "59c40419-693a-446b-bce4-89f24ff9c2fe":  # "loại công nợ"
                field["value"] = df_row['loai_cong_no'] if df_row['loai_cong_no'] else None
        return json.loads(json.dumps(body, ensure_ascii=False))

    def create_task(df_row):
        main_task_payload = create_task_payload(df_row)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            LIST_ID_TASK_DESTINATION), json=main_task_payload, headers=headers, timeout=None)
        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Created parent task: {task_id}")
            id_ = df_row['Ma']
            # Update status clickup
            query = f"""
                INSERT INTO [dbo].[log_clickup_cong_no]
                    ([account_object_code]
                    ,[dtm_creation_date])
                VALUES(N'{id_}',getdate())
            """
            call_query_sql(query=query)
            print(
                f"UPDATE: Update status clickup with id: {id_} successfully")
        else:
            print("create task fail: ", res.status_code, "Error: ", res.json())

    @task
    def create_congno_clickup():
        sql_conn = hook.get_conn()
        sql = """
            select account_object_code Ma
                ,account_object_name name
                ,debt_amount so_tien_cong_no
                ,invoice_debt_amount so_tien_cong_no_hd
                ,case when loai_cong_no = 1 then N'công nợ thu' else N'công nợ trả' end loai_cong_no
            from [dbo].[3rd_misa_api_congno] 
            where cast(account_object_code as varchar) not in 
                (select cast(account_object_code as varchar) from[dbo].[log_clickup_cong_no])

        """
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    create_congno_clickup()


dag = Clickup_post_cong_no()
