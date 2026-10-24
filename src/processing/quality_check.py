import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

ALLOWED_DOMAINS = {
    "DATA_ENGINEERING_PLATFORMS", "DATA_ANALYTICS_BI", "DATA_SCIENCE",
    "MACHINE_LEARNING", "DEEP_LEARNING_SPECIALIZED", "GENERATIVE_AI_LLM",
    "RAG_SEMANTIC_SEARCH", "MLOPS_LLMOPS_AI_INFRA", "CLOUD_DATA_AI",
    "DATA_GOVERNANCE_QUALITY", "ARCHITECTURE_LEADERSHIP", "AI_RESEARCH",
    "NON_CLASSE"
}

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    spark = (
        SparkSession.builder
        .appName("TechJobs-Quality-Check")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def run_quality_checks():
    spark = create_spark_session()
    silver_path = "s3a://silver/tech_jobs"

    print(">>> [DATA QUALITY] Démarrage des contrôles d intégrité sur Silver...")
    df = spark.read.parquet(silver_path)
    total_records = df.count()

    critical_errors = []
    warnings = []

    # 1. Volume minimal requis (Bloquant)
    if total_records == 0:
        critical_errors.append("La table Silver est vide.")

    # 2. Complétude des identifiants (Bloquant)
    null_keys = df.filter(col("job_fingerprint").isNull() | col("source_job_id").isNull()).count()
    if null_keys > 0:
        critical_errors.append(f"{null_keys} enregistrements ont un fingerprint ou un job_id nul.")

    # 3. Unicité stricte SHA-256 (Bloquant)
    distinct_keys = df.select("job_fingerprint").distinct().count()
    if distinct_keys != total_records:
        critical_errors.append(f"Doublons détectés ({total_records - distinct_keys} doublons).")

    # 4. Validité de la taxonomie (Bloquant)
    distinct_domains = set([row.primary_domain for row in df.select("primary_domain").distinct().collect()])
    invalid_domains = distinct_domains - ALLOWED_DOMAINS
    if invalid_domains:
        critical_errors.append(f"Domaines non conformes trouvés : {invalid_domains}")

    # 5. Cohérence temporelle (Avertissement)
    future_dates = df.filter(col("publication_date") > current_date()).count()
    if future_dates > 0:
        warnings.append(f"{future_dates} offres ont une date postérieure à l'horloge du conteneur.")

    print("\n==========================================")
    print(">>> RAPPORT DATA QUALITY GATE :")
    print(f"    - Enregistrements audités : {total_records}")
    print(f"    - Statut des contrôles   : {'ÉCHEC' if critical_errors else 'SUCCÈS'}")
    
    if warnings:
        for w in warnings:
            print(f"    - [WARN] {w}")

    if critical_errors:
        for err in critical_errors:
            print(f"    - [CRITICAL] {err}")
        print("==========================================\n")
        spark.stop()
        sys.exit(1)

    print("    - Tous les critères critiques sont conformes.")
    print("==========================================\n")
    spark.stop()

if __name__ == "__main__":
    run_quality_checks()