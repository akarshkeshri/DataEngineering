from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/DataEngineering/scripts')  # add scripts path

# from x import run_x
# from y import run_y
# from z import run_z

from data_call import fetch_api
from data_transformation import transformation
from data_logging import implementing_logging
from database import database_fnc 
from main import run_all
from datetime import datetime,timedelta
import pytz


#PythonOperator(task_id='run_all', python_callable=run_all)

with DAG("my_python_pipeline", start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:

    task_x = PythonOperator(
        task_id="task_run_all",
        python_callable=run_all
    )

    # task_y = PythonOperator(
    #     task_id="task_y",
    #     python_callable=run_y
    # )

    # task_z = PythonOperator(
    #     task_id="task_z",
    #     python_callable=run_z
    # )

    task_x #>> task_y >> task_z
