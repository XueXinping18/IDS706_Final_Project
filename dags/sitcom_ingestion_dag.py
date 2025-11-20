from datetime import datetime, timedelta
import os
import subprocess
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'sitcom_ingestion_dag',
    default_args=default_args,
    description='Batch ingest sitcom episodes using Smart Splitter',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'sitcom'],
) as dag:

    def scan_and_process_videos():
        """
        Scans the resources directory for videos and transcripts,
        and runs the smart_split.py script for each pair.
        """
        # Base paths (mounted in Docker)
        project_root = Path("/app") 
        resources_dir = project_root / "resources"
        scripts_dir = project_root / "scripts"
        script_path = scripts_dir / "smart_split.py"

        if not resources_dir.exists():
            print(f"Resources directory not found: {resources_dir}")
            return

        # Walk through the directory
        for root, _, files in os.walk(resources_dir):
            for file in files:
                if file.endswith((".mkv", ".mp4")):
                    video_path = Path(root) / file
                    # Assume transcript has same stem but .srt extension
                    transcript_path = video_path.with_suffix(".srt")

                    if transcript_path.exists():
                        print(f"Found pair: {video_path.name} + {transcript_path.name}")
                        
                        # Construct command
                        cmd = [
                            "python",
                            str(script_path),
                            str(video_path),
                            "--transcript",
                            str(transcript_path),
                            "--upload"  # Enable upload for the automated pipeline
                        ]
                        
                        print(f"Running: {' '.join(cmd)}")
                        
                        # Run the script
                        # Note: In a real production env, we might want to submit this to a separate worker
                        # or use a DockerOperator, but for this course project, running locally is fine.
                        try:
                            result = subprocess.run(
                                cmd, 
                                check=True, 
                                capture_output=True, 
                                text=True,
                                env={**os.environ, "PYTHONPATH": str(project_root / "src")}
                            )
                            print(result.stdout)
                        except subprocess.CalledProcessError as e:
                            print(f"Error processing {video_path.name}: {e.stderr}")
                            # Don't raise immediately so we can try other files? 
                            # Or raise to fail the task. Let's raise.
                            raise e
                    else:
                        print(f"Skipping {video_path.name}: No matching transcript found.")

    process_task = PythonOperator(
        task_id='scan_and_process_sitcoms',
        python_callable=scan_and_process_videos,
    )
