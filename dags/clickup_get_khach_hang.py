
import json

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.helper import call_api_mutiple_pages, call_multiple_thread
from common.hook import hook
from common.variables import API_TOKEN, CLICKUP_GET_TASKS

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
    tags=["Clickup ", "Customer", " khách hàng"],
    max_active_runs=1,
)
def Clickup_get_khach_hang():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }
    ######################################### API ################################################

    def call_api_get_tasks(space_id):
        params = {
            "page": 0
        }
        id_ = int(space_id)
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers, params=params, name_url=name_url, url=CLICKUP_GET_TASKS, task_id=id_)

    @task
    def call_mutiple_process_tasks() -> list:
        sql = f"select {LIST_ID_TASK_DESTINATION} id;"
        return call_multiple_thread(sql=sql, function=call_api_get_tasks, function_name='call_mutiple_process_tasks')

    ######################################### INSERT DATA ################################################
    def insert_tasks_khach_hang(list_tasks: list, table_name: str, sql=None) -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        print("---------------------------------------------INSERT DATA----------------------------------------------------------")
        if not list_tasks:
            sql_conn.close()
            print("No data to insert.")
            return
        filtered_data = [item for item in list_tasks if not (isinstance(item, list) and len(
            item) > 0 and isinstance(item[0], dict) and item[0].get('tasks') == [] and item[0].get('last_page'))]
        data = [
            item for sublist in filtered_data for t in sublist for item in t["tasks"]]

        if not data:
            sql_conn.close()
            print("No data to insert.")
            return

        if sql:
            cursor.execute(sql)
            sql_conn.commit()
            df = pd.DataFrame(data)
        else:
            sql_select = f"SELECT DISTINCT id FROM {table_name}"
            df_sql = pd.read_sql(sql_select, sql_conn)
            df = pd.DataFrame(data)
            df = df[~df['id'].isin(df_sql['id'])]

        if df.empty:
            sql_conn.close()
            print("No new data to insert.")
            return

        columns_to_json = [
            "status", "creator", "assignees", "group_assignees", "watchers",
            "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
            "locations", "sharing", "list", "project", "folder", "space"]

        for column in columns_to_json:
            if column in df.columns:
                df[column] = df[column].apply(json.dumps)

        if 'subtasks' in df.columns:
            df['subtasks'] = df['subtasks'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '[]')
        else:
            df['subtasks'] = '[]'
        sql = f"""
            INSERT INTO {table_name} (
                id, custom_id, custom_item_id, name, text_content, description, status, orderindex, date_created, date_updated,
                date_closed, date_done, archived, creator, assignees, group_assignees, watchers, checklists, tags, parent,
                priority, due_date, start_date, points, time_estimate, custom_fields, dependencies, linked_tasks, locations,
                team_id, url, sharing, permission_level, list, project, folder, space, subtasks, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """
        values = []
        for _, row in df.iterrows():
            value = (
                str(row['id']), str(row['custom_id']), str(
                    row['custom_item_id']), str(row['name']),
                str(row['text_content']), str(
                    row['description']), str(row['status']),
                str(row['orderindex']), str(
                    row['date_created']), str(row['date_updated']),
                str(row['date_closed']), str(
                    row['date_done']), str(row['archived']),
                str(row['creator']), str(row['assignees']), str(
                    row['group_assignees']),
                str(row['watchers']), str(row['checklists']), str(
                    row['tags']), str(row['parent']),
                str(row['priority']), str(
                    row['due_date']), str(row['start_date']),
                str(row['points']), str(row['time_estimate']), str(
                    row['custom_fields']),
                str(row['dependencies']), str(
                    row['linked_tasks']), str(row['locations']),
                str(row['team_id']), str(row['url']), str(row['sharing']),
                str(row['permission_level']), str(
                    row['list']), str(row['project']),
                str(row['folder']), str(row['space']), str(row['subtasks'])
            )
            values.append(value)
        cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows into {table_name} out of {df.shape[0]} rows.")

        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_tasks_sql(list_tasks: list) -> None:
        sql_truncate = "truncate table [dbo].[3rd_clickup_tasks_comment_khach_hang]; "
        insert_tasks_khach_hang(
            list_tasks=list_tasks, table_name="[dbo].[3rd_clickup_tasks_comment_khach_hang]", sql=sql_truncate)

    ############ DAG FLOW ############

    list_tasks = call_mutiple_process_tasks()
    insert_tasks_task = insert_tasks_sql(list_tasks)
    list_tasks >> insert_tasks_task


dag = Clickup_get_khach_hang()
