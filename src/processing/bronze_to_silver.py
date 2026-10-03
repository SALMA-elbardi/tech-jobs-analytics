import os
import re
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, when, lower, concat_ws, lit, coalesce, 
    to_timestamp, year, month, sha2
)
from pyspark.sql.types import (
    StringType, BooleanType, IntegerType
)

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    if not minio_user or not minio_password:
        raise ValueError(
            "Identifiants MinIO manquants ! Vérifiez que MINIO_ROOT_USER et "
            "MINIO_ROOT_PASSWORD sont bien définis dans l'environnement."
        )

    spark = (
        SparkSession.builder
        .appName("TechJobs-Bronze-To-Silver")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def extract_duration_months(text):
    if not text:
        return None
    c = text.lower()
    
    # 1. Semaines (FR & EN) : "8 semaines", "12 weeks", "24-week"
    weeks_match = re.search(r"\b(\d+)\s*(?:semaines|semaine|weeks|week|-week)\b", c)
    if weeks_match:
        w = int(weeks_match.group(1))
        return max(1, round(w / 4))

    # 2. Fourchettes : "4 à 6 mois", "4-6 mois", "3 to 6 months"
    range_match = re.search(r"(\d+)\s*(?:à|-|to)\s*(\d+)\s*(?:mois|months|month|-month)", c)
    if range_match:
        return int(range_match.group(2))

    # 3. Durée directe en mois (FR & EN) : "6 mois", "6 months", "3-month"
    month_match = re.search(r"\b([1-9]|1[0-2])\s*(?:mois|months|month|-month)\b", c)
    if month_match:
        return int(month_match.group(1))

    return None

def categorize_internship(is_internship, duration, text):
    if not is_internship:
        return None
    c = (text or "").lower()

    # Détection PFE / Fin d'études (FR & EN)
    if re.search(r"\b(pfe|projet de fin d'études|fin d'études|fin d'etude|pré-embauche|pre-embauche|end-of-study|master thesis|graduation internship)\b", c):
        return "PFE"
    
    # Détection Alternance / Apprentissage
    if re.search(r"\b(alternance|apprentissage|contrat pro|work-study|apprenticeship)\b", c):
        return "ALTERNANCE"
    
    # Qualification selon durée
    if duration == 1 or re.search(r"\b(initiation|découverte|ouvrier|observation)\b", c):
        return "STAGE_INITIATION"
    elif duration in [2, 3] or re.search(r"\b(stage d'été|stage technique|application|summer intern|summer internship)\b", c):
        return "STAGE_APPLICATION"
    elif duration and duration >= 4:
        return "PFE"

    return "STAGE_NON_SPECIFIE"

def compute_duration_bracket(duration):
    if duration is None:
        return "Non précisée"
    if duration < 2:
        return "< 2 mois (Court / Initiation)"
    elif duration <= 3:
        return "2-3 mois (Application / Été)"
    elif duration <= 6:
        return "4-6 mois (PFE / Long)"
    else:
        return "> 6 mois"

def detect_work_mode(text):
    if not text:
        return "SUR_SITE"
    c = text.lower()
    if re.search(r"\b(full remote|100% remote|100% télétravail|télétravail total)\b", c):
        return "FULL_REMOTE"
    elif re.search(r"\b(hybride|hybrid|télétravail partiel|remote partiel)\b", c):
        return "HYBRID"
    elif re.search(r"\b(remote|télétravail)\b", c):
        return "REMOTE_POSSIBLE"
    return "SUR_SITE"

udf_duration = udf(extract_duration_months, IntegerType())
udf_category = udf(categorize_internship, StringType())
udf_bracket = udf(compute_duration_bracket, StringType())
udf_work_mode = udf(detect_work_mode, StringType())

def process_bronze_to_silver(spark):
    bronze_bucket = "s3a://bronze"
    silver_bucket = "s3a://silver/tech_jobs"

    print(">>> [1/5] Lecture multiline des données brutes Adzuna...")
    adzuna_raw = spark.read.option("multiline", "true").json(f"{bronze_bucket}/api_adzuna/*/*.json")
    adzuna_df = adzuna_raw.selectExpr("explode(records) as r")
    adzuna_unified = adzuna_df.select(
        lit("adzuna_api").alias("source"),
        col("r.id").cast(StringType()).alias("source_job_id"),
        col("r.title").alias("raw_title"),
        coalesce(col("r.company.display_name"), lit("Non spécifié")).alias("company_name"),
        coalesce(col("r.location.area")[0], lit("France")).alias("location_country"),
        coalesce(col("r.location.display_name"), lit("Non spécifié")).alias("location_city"),
        to_timestamp(col("r.created")).alias("publication_date"),
        coalesce(col("r.description"), lit("")).alias("raw_description"),
        coalesce(col("r.contract_type"), lit("")).alias("raw_contract"),
        col("r.matched_domains").alias("matched_domains"),
        col("r.matched_roles").alias("matched_roles")
    )

    print(">>> [2/5] Lecture multiline des données brutes ReKrute...")
    rekrute_raw = spark.read.option("multiline", "true").json(f"{bronze_bucket}/scraper_rekrute/*/*.json")
    rekrute_df = rekrute_raw.selectExpr("explode(records) as r")
    rekrute_unified = rekrute_df.select(
        lit("rekrute_scraper").alias("source"),
        col("r.job_url").alias("source_job_id"),
        col("r.title").alias("raw_title"),
        coalesce(col("r.company"), lit("Non spécifié")).alias("company_name"),
        lit("Maroc").alias("location_country"),
        lit("Maroc").alias("location_city"),
        to_timestamp(col("r.scraped_at")).alias("publication_date"),
        concat_ws(" ", coalesce(col("r.summary"), lit("")), coalesce(col("r.raw_metadata"), lit(""))).alias("raw_description"),
        coalesce(col("r.raw_metadata"), lit("")).alias("raw_contract"),
        col("r.matched_domains").alias("matched_domains"),
        col("r.matched_roles").alias("matched_roles")
    )

    print(">>> [3/5] Harmonisation & Application des règles métier...")
    unified_df = adzuna_unified.unionByName(rekrute_unified)
    full_text_col = concat_ws(" ", col("raw_title"), col("raw_description"), col("raw_contract"))

    # Détection booléenne sans valeur nulle
    is_intern_expr = (
        lower(full_text_col).rlike(r"\b(stage|internship|intern|stagiaire|pfe|alternance|apprentissage|apprenticeship)\b") |
        lower(col("raw_contract")).rlike(r"\b(stage|intern)\b")
    )
    is_intern_clean = coalesce(is_intern_expr, lit(False))

    contract_detected = when(lower(full_text_col).rlike(r"\b(alternance|apprentissage|apprenticeship)\b"), lit("ALTERNANCE")) \
        .when(is_intern_clean, lit("STAGE")) \
        .when(lower(full_text_col).rlike(r"\b(cdi|indéterminée)\b"), lit("CDI")) \
        .when(lower(full_text_col).rlike(r"\b(cdd|déterminée)\b"), lit("CDD")) \
        .when(lower(full_text_col).rlike(r"\b(freelance|indépendant)\b"), lit("FREELANCE")) \
        .otherwise(lit("NON_SPECIFIE"))

    is_remunerated_detected = when(
        lower(full_text_col).rlike(r"\b(rémunéré|remunere|gratification|indemnité|indemnite|stipend|salaire)\b"), lit(True)
    ).when(
        lower(full_text_col).rlike(r"\b(non rémunéré|non remunere|bénévole|unpaid)\b"), lit(False)
    ).otherwise(lit(None).cast(BooleanType()))

    enriched_df = (
        unified_df
        .withColumn("corpus", full_text_col)
        .withColumn("is_internship", is_intern_clean)
        .withColumn("contract_type", contract_detected)
        .withColumn("duration_months", udf_duration(col("corpus")))
        .withColumn("duration_bracket", udf_bracket(col("duration_months")))
        .withColumn("internship_category", udf_category(col("is_internship"), col("duration_months"), col("corpus")))
        .withColumn("is_remunerated", is_remunerated_detected)
        .withColumn("work_mode", udf_work_mode(col("corpus")))
        .withColumn("job_fingerprint", sha2(concat_ws("||", col("source"), col("source_job_id")), 256))
        .withColumn("job_title_clean", lower(col("raw_title")))
        .withColumn("primary_domain", col("matched_domains")[0])
        .withColumn("ingested_at", to_timestamp(lit(datetime.now(timezone.utc).isoformat())))
    )

    print(">>> [4/5] Déduplication globale...")
    deduplicated_df = enriched_df.dropDuplicates(["job_fingerprint"])

    final_df = (
        deduplicated_df
        .withColumn("year", coalesce(year(col("publication_date")), lit(2026)))
        .withColumn("month", coalesce(month(col("publication_date")), lit(10)))
    )

    silver_table = final_df.select(
        "job_fingerprint",
        "source",
        "source_job_id",
        "job_title_clean",
        "company_name",
        "location_country",
        "location_city",
        "publication_date",
        "contract_type",
        "is_internship",
        "internship_category",
        "duration_months",
        "duration_bracket",
        "is_remunerated",
        "work_mode",
        "primary_domain",
        "matched_domains",
        "matched_roles",
        "ingested_at",
        "year",
        "month"
    )

    print(f">>> [5/5] Écriture Parquet partitionnée dans MinIO Silver...")
    (
        silver_table.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(silver_bucket)
    )

    print(f">>> [TERMINÉ] {silver_table.count()} offres unifiées écrites avec succès dans la couche Silver !")

if __name__ == "__main__":
    spark = create_spark_session()
    process_bronze_to_silver(spark)
    spark.stop()
