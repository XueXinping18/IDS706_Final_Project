from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import asyncio
import asyncpg

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'video_ingestion_dag',
    default_args=default_args,
    description='Orchestrate video splitting and ingestion',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'video'],
) as dag:

    # Task 1: Split and Upload (The "Map" Phase)
    # Runs the smart_split.py script which now handles parallel uploads and checkpointing
    split_video = BashOperator(
        task_id='split_and_upload_video',
        bash_command="""
        python /app/scripts/smart_split.py \
            "{{ dag_run.conf.get('video_path', '/app/resources/modern_family/S01/S01E01.mkv') }}" \
            --upload \
            {% if dag_run.conf.get('transcript_path') %}
            --transcript "{{ dag_run.conf.get('transcript_path') }}"
            {% endif %}
        """,
        env={
            **os.environ,
            "PYTHONPATH": "/app/src"
        }
    )

    # Task 2: Wait for Completion (The "Reduce" Monitor)
    # Polls the DB to check if all clips generated from this video are READY
    async def check_progress(video_path):
        # This is a simplified check. In a real scenario, we need to know WHICH clips belong to this video.
        # Since smart_split.py doesn't return the list of IDs to Airflow, 
        # we might rely on the 'source_video' metadata or just trust the fire-and-forget.
        # For now, we'll just print a message, or we could query by filename pattern if needed.
        print(f"Ingestion triggered for {video_path}. Check Lark/DB for progress.")
        return True

    def wait_for_ingestion(**context):
        video_path = context['dag_run'].conf.get('video_path', 'unknown')
        print(f"Waiting for ingestion of {video_path} to complete...")
        # In a real implementation, you would query the DB here.
        # For the MVP, we just acknowledge the trigger.
        return "Ingestion Triggered"

    monitor_ingestion = PythonOperator(
        task_id='monitor_ingestion_progress',
        python_callable=wait_for_ingestion,
        provide_context=True
    )

    split_video >> monitor_ingestion
