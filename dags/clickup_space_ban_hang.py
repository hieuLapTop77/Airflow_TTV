import json

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from common.helper import (
    call_api_get_list,
    call_api_mutiple_pages,
    call_multiple_thread,
    init_date,
)
from common.hook import hook
from common.variables import (
    API_TOKEN,
    CLICKUP_GET_CUSTOM_FIELDS,
    CLICKUP_GET_FOLDER_DETAILS,
    CLICKUP_GET_FOLDERS,
    CLICKUP_GET_LIST_DETAILS,
    CLICKUP_GET_LISTS,
    CLICKUP_GET_LISTS_FOLDERLESS,
    CLICKUP_GET_TASK_DETAILS,
    CLICKUP_GET_TASKS,
    ID_CLICKUP_SPACE_BAN_HANG,
)

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup", "Ban hang", 'space'],
    max_active_runs=1,
)
def Clickup_Space_Ban_hang():
    headers = {
        "Authorization": f"{API_TOKEN}",
        "Content-Type": "application/json",
    }
    ######################################### API ################################################

    def call_api_get_folders() -> list:
        sql = f"select * from [dbo].[3rd_clickup_list_spaces] where id = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_api_get_list(sql=sql, url=CLICKUP_GET_FOLDERS, headers=headers)

    def call_api_get_folder_details() -> dict:
        sql = f"select * from [dbo].[3rd_clickup_folders] where json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_api_get_list(sql=sql, url=CLICKUP_GET_FOLDER_DETAILS, headers=headers)

    def call_api_get_lists() -> list:
        sql = f"select * from [dbo].[3rd_clickup_folders] where json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_api_get_list(sql=sql, url=CLICKUP_GET_LISTS, headers=headers)

    def call_api_get_lists_by_space() -> list:
        sql = f"select * from [dbo].[3rd_clickup_list_spaces] where id = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_api_get_list(sql=sql, url=CLICKUP_GET_LISTS_FOLDERLESS, headers=headers)

    def call_api_get_list_details() -> dict:
        sql = f"select * from [dbo].[3rd_clickup_lists] where json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_api_get_list(sql=sql, url=CLICKUP_GET_LIST_DETAILS, headers=headers)

    def call_api_get_tasks(space_id):
        # date = init_date()
        params = {
            "page": 0,
            # "date_updated_gt": date["date_from"],
            # "date_updated_lt": date["date_to"]
        }
        print("params: ", params)
        name_url = 'CLICKUP_GET_TASKS'
        return call_api_mutiple_pages(headers=headers, params=params, name_url=name_url, url=CLICKUP_GET_TASKS, task_id=space_id)

    def call_mutiple_process_tasks():
        sql = f"select * from [dbo].[3rd_clickup_list_spaces] where id = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_multiple_thread(sql=sql, function=call_api_get_tasks, function_name='call_api_get_tasks')

    def call_mutiple_process_tasks_by_list() -> list:
        sql = f"select * from [dbo].[3rd_clickup_lists] where json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        return call_multiple_thread(sql=sql, function=call_api_get_tasks, function_name='call_mutiple_process_tasks_by_list')

    def call_api_get_task_details(task_id):
        params = {
            'include_subtasks': 'true',
            "page": 0
        }
        name_url = 'CLICKUP_GET_TASKS_DETAILS'
        return call_api_mutiple_pages(headers=headers, params=params, name_url=name_url, url=CLICKUP_GET_TASK_DETAILS, task_id=task_id)

    def call_mutiple_process_task_details() -> list:
        sql = f"select id from [3rd_clickup_tasks] where dtm_Creation_Date >= DATEADD(hour, -3, GETDATE()) and json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}' order by dtm_Creation_Date desc;"
        return call_multiple_thread(sql=sql, function=call_api_get_task_details, function_name='call_api_get_task_details')

    def call_api_get_custom_fields(space_id):
        params = {
            "page": 0
        }
        name_url = 'CLICKUP_GET_CUSTOM_FIELDS'
        return call_api_mutiple_pages(headers=headers, params=params, name_url=name_url, url=CLICKUP_GET_CUSTOM_FIELDS, task_id=space_id)

    def call_mutiple_process_custom_fields() -> list:
        sql = f"select distinct id from [dbo].[3rd_clickup_lists] where json_value(space, '$.id') = '{ID_CLICKUP_SPACE_BAN_HANG}';"
        print(sql)
        return call_multiple_thread(sql=sql, function=call_api_get_custom_fields, function_name='call_api_get_custom_fields')

    ######################################### INSERT DATA ################################################

    @task
    def insert_folders() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_folders = call_api_get_folders()
        if not list_folders:
            sql_conn.close()
            return

        data = [item for sublist in [d["folders"]
                                     for d in list_folders] for item in sublist]

        if not data:
            print("No data to process.")
            sql_conn.close()
            return

        ids_to_delete = [item['id'] for item in data]
        if ids_to_delete:
            ids_to_delete_str = ','.join(ids_to_delete)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_folders] WHERE id IN ({ids_to_delete_str});"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)

        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = ["lists", "statuses", "space"]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: json.dumps(x))

        required_columns = [
            'id', 'name', 'orderindex', 'override_statuses', 'hidden', 'space',
            'task_count', 'archived', 'statuses', 'lists', 'permission_level'
        ]

        for column in required_columns:
            if column not in df.columns:
                df[column] = None

        df = df[required_columns]

        sql = """
            INSERT INTO [dbo].[3rd_clickup_folders] (
                id, name, orderindex, override_statuses, hidden, space,
                task_count, archived, statuses, lists, permission_level, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [tuple(str(row[col]) if row[col] is not None else 'NULL' for col in required_columns)
                  for _, row in df.iterrows()]

        try:
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            sql_conn.close()

    @task
    def insert_folder_details() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_folder_details = call_api_get_folder_details()
        if not list_folder_details:
            print("No data to process.")
            sql_conn.close()
            return

        ids_to_delete = [item['id'] for item in list_folder_details]
        if ids_to_delete:
            ids_to_delete_str = ','.join(ids_to_delete)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_folder_details] WHERE id IN ({ids_to_delete_str});"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(list_folder_details)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return
        json_columns = ["statuses", "lists", "space"]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: json.dumps(x) if x is not None else 'NULL')

        required_columns = [
            'id', 'name', 'orderindex', 'override_statuses', 'hidden', 'space',
            'task_count', 'archived', 'statuses', 'lists', 'permission_level'
        ]

        for column in required_columns:
            if column not in df.columns:
                df[column] = None

        df = df[required_columns]

        sql = """
            INSERT INTO [dbo].[3rd_clickup_folder_details] (
                id, name, orderindex, override_statuses, hidden, space,
                task_count, archived, statuses, lists, permission_level, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            tuple(str(row[col]) if row[col]
                  is not None else 'NULL' for col in required_columns)
            for _, row in df.iterrows()
        ]

        try:
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            sql_conn.close()

    @task
    def insert_lists() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_lists = call_api_get_lists()
        if not list_lists:
            print("No data to process.")
            sql_conn.close()
            return
        data = [item for sublist in [d["lists"]
                                     for d in list_lists] for item in sublist]
        if not data:
            print("No data to process.")
            sql_conn.close()
            return

        ids_to_delete = [item['id'] for item in data]
        if ids_to_delete:
            ids_to_delete_str = ','.join(ids_to_delete)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_lists] WHERE id IN ({ids_to_delete_str});"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = ["status", "folder", "assignee", "space"]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: json.dumps(x) if x is not None else 'NULL')

        if 'content' not in df.columns:
            df['content'] = None

        cols = list(df.columns)
        if 'content' in cols:
            cols.insert(cols.index('orderindex') + 1,
                        cols.pop(cols.index('content')))
        df = df[cols]

        sql = """
            INSERT INTO [dbo].[3rd_clickup_lists](
                id, name, orderindex, content, status, priority, assignee,
                task_count, due_date, start_date, folder, space, archived,
                override_statuses, permission_level, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']),
                str(row['name']),
                str(row['orderindex']),
                str(row['content']) if row['content'] is not None else 'NULL',
                str(row['status']),
                str(row['priority']),
                str(row['assignee']),
                str(row['task_count']),
                str(row['due_date']),
                str(row['start_date']),
                str(row['folder']),
                str(row['space']),
                str(row['archived']),
                str(row['override_statuses']),
                str(row['permission_level'])
            )
            for _, row in df.iterrows()
        ]

        try:
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            sql_conn.close()

    @task
    def insert_lists_by_space() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_lists_by_space = call_api_get_lists_by_space()
        if not list_lists_by_space:
            print("No data to process.")
            sql_conn.close()
            return
        data = [item for sublist in [d["lists"]
                                     for d in list_lists_by_space] for item in sublist]
        if not data:
            print("No data to process.")
            sql_conn.close()
            return

        ids_to_delete = [item['id'] for item in data]
        if ids_to_delete:
            ids_to_delete_str = ','.join(ids_to_delete)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_lists] WHERE id IN ({ids_to_delete_str});"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = ["status", "folder", "assignee", "space"]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: json.dumps(x) if x is not None else 'NULL')

        if 'content' not in df.columns:
            df['content'] = None

        cols = list(df.columns)
        if 'content' in cols:
            cols.insert(cols.index('orderindex') + 1,
                        cols.pop(cols.index('content')))
        df = df[cols]

        sql = """
            INSERT INTO [dbo].[3rd_clickup_lists](
                id, name, orderindex, content, status, priority, assignee,
                task_count, due_date, start_date, folder, space, archived,
                override_statuses, permission_level, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']),
                str(row['name']),
                str(row['orderindex']),
                str(row['content']) if row['content'] is not None else 'NULL',
                str(row['status']),
                str(row['priority']),
                str(row['assignee']),
                str(row['task_count']),
                str(row['due_date']),
                str(row['start_date']),
                str(row['folder']),
                str(row['space']),
                str(row['archived']),
                str(row['override_statuses']),
                str(row['permission_level'])
            )
            for _, row in df.iterrows()
        ]

        try:
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            sql_conn.close()

    @task
    def insert_list_details() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_list_details = call_api_get_list_details()
        if not list_list_details:
            print("No new data to insert.")
            sql_conn.close()
            return
        ids_to_delete = [item['id'] for item in list_list_details]
        if ids_to_delete:
            ids_to_delete_str = ','.join(ids_to_delete)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_list_details] WHERE id IN ({ids_to_delete_str});"
            print(sql_del)
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(list_list_details)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = ["folder", "space", "assignee", "statuses"]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda x: json.dumps(x) if x is not None else 'NULL')

        if 'status' not in df.columns:
            df['status'] = None
        else:
            df["status"] = df["status"].apply(lambda x: json.dumps(x))

        new_col_order = [
            'id', 'name', 'deleted', 'orderindex', 'content', 'status',
            'priority', 'assignee', 'due_date', 'start_date', 'folder',
            'space', 'inbound_address', 'archived', 'override_statuses',
            'statuses', 'permission_level'
        ]
        df = df[new_col_order]

        sql = """
            INSERT INTO [dbo].[3rd_clickup_list_details](
                id, name, deleted, orderindex, content, status, priority, assignee,
                due_date, start_date, folder, space, inbound_address, archived,
                override_statuses, statuses, permission_level, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']),
                str(row['name']),
                str(row['deleted']),
                str(row['orderindex']),
                str(row['content']) if row['content'] is not None else 'NULL',
                str(row['status']),
                str(row['priority']),
                str(row['assignee']),
                str(row['due_date']),
                str(row['start_date']),
                str(row['folder']),
                str(row['space']),
                str(row['inbound_address']),
                str(row['archived']),
                str(row['override_statuses']),
                str(row['statuses']),
                str(row['permission_level'])
            )
            for _, row in df.iterrows()
        ]

        try:
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            sql_conn.close()

    @task
    def insert_tasks() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_tasks = call_mutiple_process_tasks()
        if not list_tasks:
            print("No new data to insert.")
            sql_conn.close()
            return
        filtered_data = [
            item for item in list_tasks
            if not (isinstance(item, list) and len(item) > 0 and
                    isinstance(item[0], dict) and
                    item[0].get('tasks') == [] and item[0].get('last_page'))
        ]
        data = [
            task for sublist in filtered_data for t in sublist for task in t["tasks"]]

        if data:
            ids_to_delete = ','.join(f"'{task['id']}'" for task in data)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_tasks] WHERE id IN ({ids_to_delete});"
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = [
            "status", "creator", "assignees", "group_assignees", "watchers",
            "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
            "locations", "sharing", "list", "project", "folder", "space"
        ]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: json.dumps(x))

        if 'subtasks' in df.columns:
            df['subtasks'] = df['subtasks'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '[]')
        else:
            df['subtasks'] = '[]'

        sql = """
            INSERT INTO [dbo].[3rd_clickup_tasks](
                id, custom_id, custom_item_id, name, text_content, description,
                status, orderindex, date_created, date_updated, date_closed, date_done,
                archived, creator, assignees, group_assignees, watchers, checklists,
                tags, parent, priority, due_date, start_date, points, time_estimate,
                custom_fields, dependencies, linked_tasks, locations, team_id, url,
                sharing, permission_level, list, project, folder, space, subtasks, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']), str(row['custom_id']), str(
                    row['custom_item_id']),
                str(row['name']), str(row['text_content']), str(
                    row['description']),
                str(row['status']), str(row['orderindex']), str(
                    row['date_created']),
                str(row['date_updated']), str(
                    row['date_closed']), str(row['date_done']),
                str(row['archived']), str(
                    row['creator']), str(row['assignees']),
                str(row['group_assignees']), str(
                    row['watchers']), str(row['checklists']),
                str(row['tags']), str(row['parent']), str(row['priority']),
                str(row['due_date']), str(
                    row['start_date']), str(row['points']),
                str(row['time_estimate']), str(row['custom_fields']),
                str(row['dependencies']), str(
                    row['linked_tasks']), str(row['locations']),
                str(row['team_id']), str(row['url']), str(row['sharing']),
                str(row['permission_level']), str(
                    row['list']), str(row['project']),
                str(row['folder']), str(row['space']), str(row['subtasks'])
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows into the database from {df.shape[0]} rows of DataFrame")

        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_tasks_by_list() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_tasks = call_mutiple_process_tasks_by_list()
        if not list_tasks:
            print("No new data to insert.")
            sql_conn.close()
            return
        filtered_data = [
            item for item in list_tasks
            if not (isinstance(item, list) and len(item) > 0 and
                    isinstance(item[0], dict) and
                    item[0].get('tasks') == [] and item[0].get('last_page'))
        ]
        data = [
            task for sublist in filtered_data for t in sublist for task in t["tasks"]]

        if data:
            ids_to_delete = ','.join(f"'{task['id']}'" for task in data)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_tasks] WHERE id IN ({ids_to_delete});"
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = [
            "status", "creator", "assignees", "group_assignees", "watchers",
            "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
            "locations", "sharing", "list", "project", "folder", "space"
        ]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: json.dumps(x))

        if 'subtasks' in df.columns:
            df['subtasks'] = df['subtasks'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '[]')
        else:
            df['subtasks'] = '[]'

        sql = """
            INSERT INTO [dbo].[3rd_clickup_tasks](
                id, custom_id, custom_item_id, name, text_content, description,
                status, orderindex, date_created, date_updated, date_closed, date_done,
                archived, creator, assignees, group_assignees, watchers, checklists,
                tags, parent, priority, due_date, start_date, points, time_estimate,
                custom_fields, dependencies, linked_tasks, locations, team_id, url,
                sharing, permission_level, list, project, folder, space, subtasks, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']), str(row['custom_id']), str(
                    row['custom_item_id']),
                str(row['name']), str(row['text_content']), str(
                    row['description']),
                str(row['status']), str(row['orderindex']), str(
                    row['date_created']),
                str(row['date_updated']), str(
                    row['date_closed']), str(row['date_done']),
                str(row['archived']), str(
                    row['creator']), str(row['assignees']),
                str(row['group_assignees']), str(
                    row['watchers']), str(row['checklists']),
                str(row['tags']), str(row['parent']), str(row['priority']),
                str(row['due_date']), str(
                    row['start_date']), str(row['points']),
                str(row['time_estimate']), str(row['custom_fields']),
                str(row['dependencies']), str(
                    row['linked_tasks']), str(row['locations']),
                str(row['team_id']), str(row['url']), str(row['sharing']),
                str(row['permission_level']), str(
                    row['list']), str(row['project']),
                str(row['folder']), str(row['space']), str(row['subtasks'])
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows into the database from {df.shape[0]} rows of DataFrame")

        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_task_details() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_task_details_task = call_mutiple_process_task_details()
        if not list_task_details_task:
            print("No new data to insert.")
            sql_conn.close()
            return
        list_data = [
            task for sublist in list_task_details_task for task in sublist]
        if list_data:
            ids_to_delete = ','.join(f"'{task['id']}'" for task in list_data)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_task_details] WHERE id IN ({ids_to_delete});"
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(list_data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return

        json_columns = [
            "status", "creator", "assignees", "group_assignees", "watchers",
            "checklists", "tags", "custom_fields", "dependencies", "linked_tasks",
            "locations", "sharing", "list", "project", "folder", "space", "attachments"
        ]
        for column in json_columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: json.dumps(x))

        if 'subtasks' in df.columns:
            df['subtasks'] = df['subtasks'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '[]')
        else:
            df['subtasks'] = '[]'

        if 'time_sent' in df.columns:
            df['time_sent'] = df['time_sent'].apply(
                lambda x: json.dumps(x) if isinstance(x, list) else '')
        else:
            df['time_sent'] = ''

        new_col = [
            'id', 'custom_id', 'custom_item_id', 'name', 'text_content', 'description',
            'status', 'orderindex', 'date_created', 'date_updated', 'date_closed', 'date_done',
            'archived', 'creator', 'assignees', 'group_assignees', 'watchers', 'checklists',
            'tags', 'parent', 'priority', 'due_date', 'start_date', 'points', 'time_estimate',
            'time_sent', 'custom_fields', 'dependencies', 'linked_tasks', 'locations', 'team_id',
            'url', 'sharing', 'permission_level', 'list', 'project', 'folder', 'space',
            'subtasks', 'attachments'
        ]
        df = df[new_col]
        sql = """
            INSERT INTO [dbo].[3rd_clickup_task_details](
                id, custom_id, custom_item_id, name, text_content, description, status,
                orderindex, date_created, date_updated, date_closed, date_done, archived,
                creator, assignees, group_assignees, watchers, checklists, tags, parent,
                priority, due_date, start_date, points, time_estimate, time_sent, custom_fields,
                dependencies, linked_tasks, locations, team_id, url, sharing, permission_level,
                list, project, folder, space, subtasks, attachments, [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """
        values = [
            (
                str(row['id']), str(row['custom_id']), str(
                    row['custom_item_id']),
                str(row['name']), str(row['text_content']), str(
                    row['description']),
                str(row['status']), str(row['orderindex']), str(
                    row['date_created']),
                str(row['date_updated']), str(
                    row['date_closed']), str(row['date_done']),
                str(row['archived']), str(
                    row['creator']), str(row['assignees']),
                str(row['group_assignees']), str(
                    row['watchers']), str(row['checklists']),
                str(row['tags']), str(row['parent']), str(row['priority']),
                str(row['due_date']), str(
                    row['start_date']), str(row['points']),
                str(row['time_estimate']), str(
                    row['time_sent']), str(row['custom_fields']),
                str(row['dependencies']), str(
                    row['linked_tasks']), str(row['locations']),
                str(row['team_id']), str(row['url']), str(row['sharing']),
                str(row['permission_level']), str(
                    row['list']), str(row['project']),
                str(row['folder']), str(row['space']), str(row['subtasks']),
                str(row['attachments'])
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows into the database from {df.shape[0]} rows of DataFrame")

        sql_conn.commit()
        sql_conn.close()

    @task
    def insert_custom_fields() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        list_custom_fields = call_mutiple_process_custom_fields()
        if not list_custom_fields:
            print("No new data to insert.")
            sql_conn.close()
            return
        data = [
            field for sublist in list_custom_fields for t in sublist for field in t["fields"]]

        if data:
            ids_to_delete = ','.join(f"'{field['id']}'" for field in data)
            sql_del = f"DELETE FROM [dbo].[3rd_clickup_custom_fields] WHERE id IN ({ids_to_delete});"
            cursor.execute(sql_del)
            sql_conn.commit()

        df = pd.DataFrame(data)
        if df.empty:
            print("No new data to insert.")
            sql_conn.close()
            return
        if "type_config" in df.columns:
            df["type_config"] = df["type_config"].apply(
                lambda x: json.dumps(x))

        sql = """
            INSERT INTO [dbo].[3rd_clickup_custom_fields](
                id,
                name,
                type,
                type_config,
                date_created,
                hide_from_guests,
                required,
                [dtm_Creation_Date]
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, GETDATE()
            )
        """

        values = [
            (
                str(row['id']), str(row['name']), str(
                    row['type']), str(row['type_config']),
                str(row['date_created']), str(
                    row['hide_from_guests']), str(row['required'])
            )
            for _, row in df.iterrows()
        ]
        cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows into the database from {df.shape[0]} rows of DataFrame")

        sql_conn.commit()
        sql_conn.close()

    @task
    def call_procedure() -> None:
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()

        sql = "exec [sp_Custom_Fields_List];"
        cursor.execute(sql)
        sql_conn.commit()
        sql_conn.close()

    ############ DAG FLOW ############

    insert_folders() >> insert_folder_details() >> insert_lists() >> insert_lists_by_space() >> insert_list_details(
    ) >> insert_tasks() >> insert_tasks_by_list() >> insert_task_details() >> insert_custom_fields() >> call_procedure()


dag = Clickup_Space_Ban_hang()
