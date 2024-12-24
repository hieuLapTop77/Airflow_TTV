import concurrent.futures
import copy
import json
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from common.helper import call_api_mutiple_pages, handle_df
from common.hook import hook
from common.variables import (
    API_TOKEN,
    CLICKUP_CREATE_TASK,
    CLICKUP_DELETE_TASK,
    CLICKUP_GET_TASKS,
)

# Variables
ID_LIST_DEFAULT = 901802888489

HEADERS = {
    "Authorization": f"{API_TOKEN}",
    "Content-Type": "application/json",
}

BODY_TEMPLATE = {
    "name": "",
    "text_content": "",
    "description": "",
    "status": "to do",
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
            "id": "afa22292-977f-4c33-8b7a-59017ff5bd5f",
            "name": "Mô tả",
            "value": None
        },

        {
            "id": "39d3440e-eec2-455b-8206-b7aa42a185b6",
            "name": "Ngày tạo",
            "value": None
        },
        {
            "id": "c2d9be6b-de09-4aac-899c-e9a338883900",
            "name": "Trạng thái hoạt động",
            "value": None
        },
        {
            "id": "730271d8-7809-4f0a-8ce4-50db86d709b7",
            "name": "Tên kho",
            "value": None
        },
        {
            "id": "24334fd5-61a1-4f1d-864b-f54b9b6ad5e3",
            "name": "Tạo bởi",
            "value": None
        },
        # Sản phẩm

        {
            "id": "ed374f7e-e095-4704-b206-3d4b4829b5d8",
            "name": "Giá tiền",
            "value": None
        },
        {
            "id": "2bc3f282-4535-49ef-9954-986aa9b04b1e",
            "name": "Lot No",
            "value": None
        },
        {
            "id": "d8256dd9-797f-4d1f-b01a-e3d81dd7c881",
            "name": "Mã sản phẩm",
            "value": None
        },
        {
            "id": "a1a26bfb-ebce-47fa-98f8-dac2e21a328e",
            "name": "Ngày hết hạn",
            "value": None
        },
        {
            "id": "71b3f818-0a62-4e34-810c-4f8e225a6b18",
            "name": "Số lượng",
            "value": None
        },
        {
            "id": "bbbbc74f-57d2-4f2a-a2a4-5a00afb6d427",
            "name": "Tên sản phẩm",
            "value": None
        },
        {
            "id": "18943d3e-616c-45dc-8f3d-ddba6dbf5a5a",
            "name": "Đơn giá",
            "value": None
        }
    ],
    "attachments": [],
}

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="30 */4 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "clickup", 'Tồn kho'],
    max_active_runs=1,
)
def Clickup_post_ton_kho():
    ######################################### API ################################################
    def call_api_delete(task_id) -> None:
        response = requests.delete(CLICKUP_DELETE_TASK.format(
            task_id), timeout=None, headers=HEADERS)
        print(CLICKUP_DELETE_TASK.format(task_id))
        if response.status_code in [200, 204]:
            print('Delete successful task: ', task_id)
        else:
            print("Error please check api with error: ", response.json())

    def call_api_get_tasks(list_id):
        params = {
            "page": 0
        }
        name_url = 'CLICKUP_GET_TASKS'
        print(list_id)
        return call_api_mutiple_pages(headers=HEADERS, params=params, name_url=name_url, url=CLICKUP_GET_TASKS, task_id=list_id)

    def call_mutiple_thread_tasks():
        list_tasks: list = call_api_get_tasks(ID_LIST_DEFAULT)
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(
            item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [item for sublist in filtered_data for item in sublist["tasks"]]
        return pd.DataFrame(data).to_json()

    def create_task_payload(df_row):
        body = copy.deepcopy(BODY_TEMPLATE)
        body['name'] = df_row['Ten_san_pham']
        body['date_created'] = int(datetime.now().timestamp() * 1000)
        for field in body["custom_fields"]:
            if field["id"] == "24334fd5-61a1-4f1d-864b-f54b9b6ad5e3":  # "Tạo bởi"
                field["value"] = df_row['created_by'] if df_row['created_by'] is not None else None

            elif field["id"] == "730271d8-7809-4f0a-8ce4-50db86d709b7":  # "Mã kho"
                field["value"] = df_row['ma_kho'] if df_row['ma_kho'] is not None else None

            elif field["id"] == "39d3440e-eec2-455b-8206-b7aa42a185b6":  # "Ngày tạo"
                field["value"] = df_row['created_date'] if df_row['created_date'] != 0 else None

            elif field["id"] == "afa22292-977f-4c33-8b7a-59017ff5bd5f":  # "mô tả"
                field["value"] = df_row['description'] if df_row['description'] is not None else None

            elif field["id"] == "c2d9be6b-de09-4aac-899c-e9a338883900":  # "Trạng thái hoạt động"
                field["value"] = df_row['trang_thai'] if df_row['trang_thai'] is not None else None
            # Sản phẩm

            elif field["id"] == "ed374f7e-e095-4704-b206-3d4b4829b5d8":  # "Giá tiền"
                field["value"] = df_row['Gia_tien'] if df_row['Gia_tien'] is not None else None

            elif field["id"] == "2bc3f282-4535-49ef-9954-986aa9b04b1e":  # "Lot No"
                field["value"] = df_row['lot_no'] if df_row['lot_no'] is not None else None

            elif field["id"] == "d8256dd9-797f-4d1f-b01a-e3d81dd7c881":  # "Mã sản phẩm"
                field["value"] = df_row['Ma_san_pham'] if df_row['Ma_san_pham'] is not None else None

            elif field["id"] == "a1a26bfb-ebce-47fa-98f8-dac2e21a328e":  # "Ngày hết hạn"
                field["value"] = df_row['Ngay_het_han'] if df_row['Ngay_het_han'] != 0 else None

            elif field["id"] == "71b3f818-0a62-4e34-810c-4f8e225a6b18":  # "Số lượng"
                field["value"] = df_row['So_luong'] if df_row['So_luong'] is not None else None

            elif field["id"] == "bbbbc74f-57d2-4f2a-a2a4-5a00afb6d427":  # "Tên kho"
                field["value"] = df_row['ten_kho'] if df_row['ten_kho'] is not None else None

            elif field["id"] == "18943d3e-616c-45dc-8f3d-ddba6dbf5a5a":  # "Đơn giá"
                field["value"] = df_row['Don_gia'] if df_row['Don_gia'] is not None else None

        return json.loads(json.dumps(body, ensure_ascii=False))

    def create_tonkho_clickup_task(df_row):
        main_task_payload = create_task_payload(df_row)
        # print(main_task_payload)
        res = requests.post(CLICKUP_CREATE_TASK.format(
            ID_LIST_DEFAULT), json=main_task_payload, headers=HEADERS, timeout=None)
        print('--------------------------------------', res.status_code)
        while res.status_code == 429:
            main_task_payload = create_task_payload(df_row)
            # print(main_task_payload)
            res = requests.post(CLICKUP_CREATE_TASK.format(
                ID_LIST_DEFAULT), json=main_task_payload, headers=HEADERS, timeout=None)
            print(f"Error: {res.status_code}, text: {res.json()}")

        if res.status_code == 200:
            task_id = res.json()['id']
            print(f"Task id: {task_id} successfully created")
        else:
            print(f"Error: {res.status_code}, text: {res.json()}")
            # raise RuntimeError(
            #     f"Task creation failed with status code {res.status_code}: {res.text}")

    @task
    def delete_tasks() -> None:
        df_tasks = call_mutiple_thread_tasks()
        df_tasks = handle_df(df_tasks)
        if df_tasks.empty:
            return
        task_ids_to_delete = df_tasks['id'].tolist()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(call_api_delete, task_ids_to_delete)

    @task
    def create_tonkho_clickup() -> None:
        sql_conn = hook.get_conn()
        sql = """select * from [dbo].[vw_Clickup_ton_kho];
            """
        df = pd.read_sql(sql, sql_conn)
        print(len(df))
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(create_tonkho_clickup_task, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    # delete_tasks()
    # delete_tasks() >> create_tonkho_clickup()
    create_tonkho_clickup()


dag = Clickup_post_ton_kho()
