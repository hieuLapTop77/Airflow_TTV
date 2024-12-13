from airflow.decorators import dag, task
from airflow.models import DagBag, XCom
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session

default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="30 */12 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Delete Xcom", "Xcom"],
    max_active_runs=1,
)
def Delete_Xcom():

    @task
    def clear_all_dags_xcoms():
        dagbag = DagBag()
        all_dag_ids = dagbag.dags.keys()

        @provide_session
        def delete_xcom(dag_id, session=None):
            session.query(XCom).filter(XCom.dag_id == dag_id).delete()
            session.commit()

        for dag_id in all_dag_ids:
            delete_xcom(dag_id)

    clear_all_dags_xcoms()


dag = Delete_Xcom()
