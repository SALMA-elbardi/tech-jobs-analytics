import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def on_failure_notify(context):
    try:
        from src.utils.alerts import airflow_failure_callback
        airflow_failure_callback(context)
    except Exception as e:
        print(f"Erreur callback alerte : {e}")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    
}

with DAG(
    dag_id='tech_jobs_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline Lakehouse Medallion pour le marché tech marocain',
    schedule_interval='0 2 * * *',  # Exécution quotidienne automatique à 02:00 AM UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,  # Évite de rejouer tout l'historique depuis le start_date
    tags=['tech_jobs', 'lakehouse', 'medallion', 'spark'],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # -------------------------------------------------------------
    # 1. PHASE BRONZE : Ingestion parallèle des 4 sources avec CDC
    # -------------------------------------------------------------
    extract_adzuna = BashOperator(
        task_id="extract_adzuna_api",
        bash_command="python /opt/airflow/src/ingestion/extract_adzuna_cdc.py"
    )

    extract_rekrute = BashOperator(
        task_id="extract_rekrute_scraper",
        bash_command="python /opt/airflow/src/ingestion/extract_rekrute_scraper.py"
    )

    extract_emploima = BashOperator(
        task_id="extract_emploima_scraper",
        bash_command="python /opt/airflow/src/ingestion/extract_emploima_scraper.py"
    )

    extract_dreamjob = BashOperator(
        task_id="extract_dreamjob_scraper",
        bash_command="python /opt/airflow/src/ingestion/extract_dreamjob_scraper.py"
    )

    bronze_sync = EmptyOperator(task_id="bronze_sync_gate")

    # -------------------------------------------------------------
    # 2. PHASE SILVER : Spark Bronze to Silver
    # -------------------------------------------------------------
    spark_bronze_to_silver = BashOperator(
        task_id="spark_transform_silver",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master 'local[*]' "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark/work-dir/src/processing/bronze_to_silver.py"
        )
    )

    # -------------------------------------------------------------
    # 3.  Data Quality Gate
    # -------------------------------------------------------------
    quality_gate_check = BashOperator(
        task_id="data_quality_gate",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master 'local[*]' "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark/work-dir/src/processing/quality_check.py"
        )
    )

    # -------------------------------------------------------------
    # 4. PHASE GOLD : Spark Silver to Gold (Star Schema)
    # -------------------------------------------------------------
    spark_silver_to_gold = BashOperator(
        task_id="spark_transform_gold",
        bash_command=(
            "docker exec spark /opt/spark/bin/spark-submit "
            "--master 'local[*]' "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark/work-dir/src/processing/silver_to_gold.py"
        )
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Ordonnancement des dépendances
    start_pipeline >> [extract_adzuna, extract_rekrute, extract_emploima, extract_dreamjob] >> bronze_sync
    bronze_sync >> spark_bronze_to_silver >> quality_gate_check >> spark_silver_to_gold >> end_pipeline