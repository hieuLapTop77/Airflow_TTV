import concurrent.futures

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from common.helper import call_query_sql
from common.hook import hook
from common.variables import API_TOKEN, CLICKUP_COMMENT

# Variables
LIST_ID_TASK_DESTINATION = 901802834969
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
    tags=["Clickup comment ", " comment", " khách hàng"],
    max_active_runs=1,
)
def Clickup_Comment_khach_hang():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }
    ######################################### API ################################################

    def post_comment(df_row):
        task_id = df_row["id"]
        so_chung_tu = df_row["So_chung_tu"]
        url_comment = CLICKUP_COMMENT.format(task_id)
        body = {
            "comment_text": df_row["content"],
            "assignee": None,
            "group_assignee": None,
            "notify_all": True
        }
        sql = f"""
            Insert into 
                [dbo].[log_clickup_comment_khach_hang] (
                        [So_chung_tu]
                        ,[dtm_creation_date]
                    )values ('{so_chung_tu}', getdate())
        """
        headers['Content-Type'] = 'application/json'
        response_comment = requests.post(
            url_comment, headers=headers, json=body, timeout=None)

        if response_comment.status_code == 200:
            call_query_sql(query=sql)
            print('COMMENT: Comment added successfully.')
        else:
            print(
                f'COMMENT: Failed {response_comment.status_code} to add comment: {response_comment.json()} on task: {task_id}')

    @task
    def create_comment_clickup():
        sql_conn = hook.get_conn()
        sql = "select * from [dbo].[vw_Clickup_Comment_Khach_Hang] where So_chung_tu not in (select So_chung_tu from [dbo].[log_clickup_comment_khach_hang]) order by Ngay_hach_toan;"
        df = pd.read_sql(sql, sql_conn)
        sql_conn.close()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(post_comment, [
                         row for _, row in df.iterrows()])

    ############ DAG FLOW ############
    create_comment_clickup()


dag = Clickup_Comment_khach_hang()
