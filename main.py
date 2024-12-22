import time
import random
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Function for selecting a medal
def pick_medal():
    return random.choice(['calc_Bronze', 'calc_Silver', 'calc_Gold'])

# Delay function
def generate_delay():
    time.sleep(35)

# DAG parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medal_dag',
    default_args=default_args,
    description='DAG for Olympic Medal Calculation',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 1: Create a table
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='mysql_default',
        sql="""
        CREATE TABLE IF NOT EXISTS medals_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(50),
            count INT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2: Choosing a medal
    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal,
    )

    # Task 3.1: Calculation for Bronze
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='mysql_default',
        sql="""
        INSERT INTO medals_table (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    # Task 3.2: Calculation for Silver
    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='mysql_default',
        sql="""
        INSERT INTO medals_table (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    # Task 3.3: Calculation for Gold
    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='mysql_default',
        sql="""
        INSERT INTO medals_table (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # Task 4: Delay
    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
    )

    # Task 5: Sensor
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id='mysql_default',
        sql="""
        SELECT 1
        FROM medals_table
        WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        mode='poke',
        poke_interval=10,
        timeout=60,
    )

    # Defining dependencies
    create_table >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task >> check_for_correctness

