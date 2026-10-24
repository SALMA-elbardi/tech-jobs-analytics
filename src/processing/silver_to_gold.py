import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, explode_outer, 
    to_date, coalesce, when, monotonically_increasing_id
)

def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    spark = (
        SparkSession.builder
        .appName("TechJobs-Silver-To-Gold")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    return spark

def process_silver_to_gold(spark):
    bucket_silver = os.getenv("MINIO_SILVER_BUCKET", "silver")
    bucket_gold = os.getenv("MINIO_GOLD_BUCKET", "gold")
    
    silver_path = f"s3a://{bucket_silver}/"
    gold_base_path = f"s3a://{bucket_gold}"

    print(f">>> [GOLD] Lecture des données Silver depuis {silver_path}...")
    df_silver = spark.read.parquet(silver_path).cache()

    # ---------------------------------------------------------
    # 1. DIMENSION ENTREPRISE (dim_company)
    # ---------------------------------------------------------
    dim_company = (
        df_silver
        .groupBy("company_name")
        .agg(count("job_fingerprint").alias("total_jobs_posted"))
        .withColumn("company_id", monotonically_increasing_id() + 1)
        .select("company_id", "company_name", "total_jobs_posted")
    ).cache()

    # ---------------------------------------------------------
    # 2. DIMENSION LOCALISATION (dim_location)
    # ---------------------------------------------------------
    dim_location = (
        df_silver
        .select("location_country", "location_city")
        .distinct()
        .withColumn("location_id", monotonically_increasing_id() + 1)
        .withColumn(
            "is_morocco", 
            when(col("location_country") == "Maroc", True).otherwise(False)
        )
        .select("location_id", "location_country", "location_city", "is_morocco")
    ).cache()

    # ---------------------------------------------------------
    # 3. DIMENSION TAXONOMIE & COMPÉTENCES (dim_taxonomy)
    # ---------------------------------------------------------
    dim_taxonomy = (
        df_silver
        .select(
            col("primary_domain").alias("domain"),
            explode_outer(col("matched_roles")).alias("role_keyword")
        )
        .distinct()
        .withColumn("role_keyword", coalesce(col("role_keyword"), lit("Generaliste")))
        .withColumn("taxonomy_id", monotonically_increasing_id() + 1)
        .select("taxonomy_id", "domain", "role_keyword")
    ).cache()

    # ---------------------------------------------------------
    # 4. TABLE DE FAITS GLOBALE (fact_job_postings)
    # ---------------------------------------------------------
    fact_jobs = (
        df_silver
        .join(dim_company, on="company_name", how="left")
        .join(dim_location, on=["location_country", "location_city"], how="left")
        .select(
            col("job_fingerprint"),
            col("source"),
            col("source_job_id").alias("job_url"),
            col("job_title_clean").alias("job_title"),
            col("company_id"),
            col("location_id"),
            to_date(col("publication_date")).alias("publication_date"),
            col("contract_type"),
            col("work_mode"),
            col("is_internship"),
            col("is_remunerated"),
            col("primary_domain"),
            col("year"),
            col("month")
        )
    )

    # ---------------------------------------------------------
    # 5. DATA MART SPÉCIALISÉ : STAGES & PFE (fact_internships_pfe)
    # ---------------------------------------------------------
    fact_stages = (
        df_silver
        .filter(col("is_internship") == True)
        .join(dim_company, on="company_name", how="left")
        .join(dim_location, on=["location_country", "location_city"], how="left")
        .select(
            col("job_fingerprint"),
            col("source"),
            col("source_job_id").alias("job_url"),
            col("job_title_clean").alias("job_title"),
            col("company_id"),
            col("location_id"),
            to_date(col("publication_date")).alias("publication_date"),
            col("internship_category"),
            col("duration_months"),
            col("duration_bracket"),
            col("is_remunerated"),
            col("work_mode"),
            col("primary_domain"),
            col("year"),
            col("month")
        )
    )

    # ---------------------------------------------------------
    # PERSISTANCE DANS MINIO GOLD (Format Parquet Snappy)
    # ---------------------------------------------------------
    print(">>> [GOLD] Écriture des dimensions...")
    dim_company.write.mode("overwrite").parquet(f"{gold_base_path}/dim_company")
    dim_location.write.mode("overwrite").parquet(f"{gold_base_path}/dim_location")
    dim_taxonomy.write.mode("overwrite").parquet(f"{gold_base_path}/dim_taxonomy")

    print(">>> [GOLD] Écriture des tables de faits...")
    fact_jobs.write.mode("overwrite").partitionBy("year", "month").parquet(f"{gold_base_path}/fact_job_postings")
    fact_stages.write.mode("overwrite").partitionBy("year", "month").parquet(f"{gold_base_path}/fact_internships_pfe")

    print("\n==========================================")
    print(">>> SUCCÈS COUCHE GOLD :")
    print(f"    - dim_company          : {dim_company.count()} entreprises uniques")
    print(f"    - dim_location         : {dim_location.count()} localisations uniques")
    print(f"    - fact_job_postings    : {fact_jobs.count()} lignes de faits globales")
    print(f"    - fact_internships_pfe : {fact_stages.count()} opportunités de stages qualifiées")
    print("==========================================\n")

if __name__ == "__main__":
    spark = create_spark_session()
    process_silver_to_gold(spark)
    spark.stop()