import os
import re
import json
import logging
from datetime import datetime, timezone
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Variables d'environnement
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

if not MINIO_USER or not MINIO_PASSWORD:
    raise ValueError("Identifiants MinIO manquants dans le fichier .env !")

if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
    raise ValueError("ADZUNA_APP_ID et ADZUNA_APP_KEY manquants dans le fichier .env !")

STATE_KEY = "_state/adzuna_seen_ids.json"
MAX_JOB_AGE_DAYS = 60

# Requêtes élargies intégrant postes confirmés ET stages/alternances
SEARCH_TERMS = [
    # Postes Data / IA
    "data engineer", "data analyst", "business intelligence", "power bi",
    "machine learning", "data scientist", "ai engineer", "mlops", "generative ai",
    # Requêtes spécifiques Stages, PFE & Alternances
    "stage data", "stage pfe data", "stage business intelligence",
    "stage machine learning", "data intern", "alternance data", "alternance ia"
]

DATA_AI_TAXONOMY = {
    "DATA_ENGINEERING_PLATFORMS": [
        "data engineer", "big data engineer", "cloud data engineer", "data platform engineer",
        "data infrastructure engineer", "data warehouse engineer", "data lake engineer",
        "lakehouse engineer", "etl developer", "elt developer", "streaming data engineer",
        "data integration engineer", "dataops engineer", "database engineer", "database administrator",
        "dba", "data architect", "cloud data architect", "pyspark", "spark", "kafka", "airflow", "dbt"
    ],
    "DATA_ANALYTICS_BI": [
        "data analyst", "business intelligence analyst", "bi analyst", "bi developer",
        "bi engineer", "analytics engineer", "business analyst", "product data analyst",
        "product analyst", "marketing data analyst", "financial data analyst", "risk data analyst",
        "operations analyst", "customer analyst", "crm analyst", "web analyst", "digital analyst",
        "data visualization specialist", "decision scientist", "power bi", "tableau", "looker"
    ],
    "DATA_SCIENCE": [
        "data scientist", "applied data scientist", "senior data scientist", "lead data scientist",
        "statistical data scientist", "research data scientist", "product data scientist",
        "quantitative analyst", "quant", "statistician", "experimentation scientist",
        "ab testing scientist", "optimization scientist", "operations research scientist"
    ],
    "MACHINE_LEARNING": [
        "machine learning engineer", "ml engineer", "machine learning scientist",
        "applied machine learning engineer", "ml software engineer", "ml infrastructure engineer",
        "ml platform engineer", "mlops engineer", "model deployment engineer",
        "model optimization engineer", "ml research engineer", "applied scientist"
    ],
    "DEEP_LEARNING_SPECIALIZED": [
        "deep learning engineer", "nlp engineer", "nlp scientist", "computer vision engineer",
        "computer vision scientist", "speech ml engineer", "audio ml engineer",
        "recommendation systems engineer", "search engineer", "ranking engineer",
        "recommender systems scientist", "time series scientist", "forecasting scientist",
        "fraud detection ml engineer", "robotics ml engineer", "autonomous systems engineer",
        "reinforcement learning engineer", "graph machine learning engineer"
    ],
    "GENERATIVE_AI_LLM": [
        "generative ai engineer", "genai engineer", "llm engineer", "ai engineer",
        "applied ai engineer", "generative ai developer", "llm application engineer",
        "llm infrastructure engineer", "llmops engineer", "rag engineer",
        "conversational ai engineer", "ai agent engineer", "agentic ai engineer",
        "ai platform engineer", "ai solutions engineer", "ai solutions architect",
        "ai research engineer", "generative ai research scientist", "prompt engineer"
    ],
    "RAG_SEMANTIC_SEARCH": [
        "ai search engineer", "semantic search engineer", "information retrieval engineer",
        "knowledge engineer", "knowledge graph engineer", "vector search engineer",
        "chatbot engineer", "vector database", "embeddings"
    ],
    "MLOPS_LLMOPS_AI_INFRA": [
        "model serving engineer", "ml devops engineer", "ml reliability engineer",
        "ai reliability engineer", "feature store", "model registry", "mlflow"
    ],
    "CLOUD_DATA_AI": [
        "cloud ai engineer", "cloud ml engineer", "cloud solutions architect data ai",
        "cloud platform engineer", "data ai solutions architect", "databricks",
        "snowflake", "bigquery", "synapse", "data factory"
    ],
    "DATA_GOVERNANCE_QUALITY": [
        "data governance analyst", "data governance specialist", "data quality analyst",
        "data quality engineer", "data steward", "master data management specialist",
        "mdm specialist", "metadata management specialist", "data privacy specialist",
        "data security engineer", "data compliance analyst", "data governance manager"
    ],
    "ARCHITECTURE_LEADERSHIP": [
        "big data architect", "ai architect", "ml architect", "generative ai architect",
        "data ai architect", "solutions architect data ai", "enterprise data architect",
        "lead data engineer", "lead ml engineer", "data engineering manager",
        "data science manager", "ai engineering manager", "head of data",
        "head of data engineering", "head of ai", "chief data officer", "cdo",
        "chief ai officer", "caio"
    ],
    "AI_RESEARCH": [
        "ai research scientist", "machine learning research scientist", "deep learning researcher",
        "nlp research scientist", "computer vision research scientist",
        "generative ai research scientist", "llm research scientist", "research engineer"
    ]
}

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def load_seen_ids(s3):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return set(data.get("seen_ids", []))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return set()
        raise e

def save_seen_ids(s3, seen_ids):
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_seen": len(seen_ids),
        "seen_ids": list(seen_ids)
    }
    s3.put_object(Bucket=BUCKET_NAME, Key=STATE_KEY, Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")

def is_recent_job(created_str, max_days=MAX_JOB_AGE_DAYS):
    if not created_str:
        return True
    try:
        clean = created_str.split(".")[0].replace("Z", "")
        job_dt = datetime.fromisoformat(clean).replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - job_dt).days <= max_days
    except Exception:
        return True

def parse_internship_and_work_mode(title, description, raw_contract_type):
    """Détecte les attributs spécifiques aux stages et au mode de travail."""
    corpus = f"{title} {description}".lower()

    # 1. Détection Stage / Alternance
    is_internship = False
    internship_type = "CDI_OR_OTHER"

    if re.search(r"\b(pfe|projet de fin d'études|fin d'etude)\b", corpus):
        is_internship = True
        internship_type = "STAGE_PFE"
    elif re.search(r"\b(stage|internship|intern|stagiaire)\b", corpus) or (raw_contract_type and "intern" in raw_contract_type.lower()):
        is_internship = True
        internship_type = "STAGE"
    elif re.search(r"\b(alternance|apprentissage|contrat pro)\b", corpus):
        is_internship = True
        internship_type = "ALTERNANCE"

    # 2. Détection Durée du stage (ex: 6 mois, 4-6 mois)
    duration_match = re.search(r"(\d+)\s*(?:à|-)?\s*(\d+)?\s*(?:mois|months)", corpus)
    internship_duration = duration_match.group(0) if duration_match else None

    # 3. Rémunération
    is_remunerated = None
    if re.search(r"\b(rémunéré|remunere|gratification|indemnité|indemnite|stipend|salary)\b", corpus):
        is_remunerated = True
    elif re.search(r"\b(non rémunéré|non remunere|bénévole|unpaid)\b", corpus):
        is_remunerated = False

    # 4. Mode de travail (Remote / Hybride / Sur site)
    work_mode = "SUR_SITE"
    if re.search(r"\b(full remote|100% remote|100% télétravail|télétravail total)\b", corpus):
        work_mode = "FULL_REMOTE"
    elif re.search(r"\b(hybride|hybrid|télétravail partiel|remote partiel)\b", corpus):
        work_mode = "HYBRID"
    elif re.search(r"\b(remote|télétravail)\b", corpus):
        work_mode = "REMOTE_POSSIBLE"

    return {
        "is_internship": is_internship,
        "internship_type": internship_type,
        "internship_duration": internship_duration,
        "is_remunerated": is_remunerated,
        "work_mode": work_mode
    }

def classify_job(title, description):
    corpus = f"{title} {description}".lower()
    matched_domains = set()
    matched_roles = set()

    for domain, roles in DATA_AI_TAXONOMY.items():
        for role in roles:
            if re.search(rf"\b{re.escape(role)}\b", corpus):
                matched_domains.add(domain)
                matched_roles.add(role)

    return list(matched_domains), list(matched_roles)

def fetch_adzuna_delta(country="fr", max_pages_per_term=2):
    s3 = get_s3_client()
    seen_ids = load_seen_ids(s3)
    logging.info(f"[CDC] Registre d'état chargé : {len(seen_ids)} IDs déjà connus dans le Data Lake.")

    delta_jobs = {}
    base_url = f"https://api.adzuna.com/v1/api/jobs/{country}/search"

    for term in SEARCH_TERMS:
        for page in range(1, max_pages_per_term + 1):
            try:
                url = f"{base_url}/{page}"
                params = {
                    "app_id": ADZUNA_APP_ID,
                    "app_key": ADZUNA_APP_KEY,
                    "what": term,
                    "results_per_page": 50,
                    "content-type": "application/json"
                }
                resp = requests.get(url, params=params, timeout=20)
                resp.raise_for_status()
                results = resp.json().get("results", [])

                for item in results:
                    jid = str(item.get("id"))
                    created = item.get("created")

                    if not jid or jid in seen_ids or not is_recent_job(created):
                        continue

                    title = item.get("title", "")
                    desc = item.get("description", "")
                    raw_contract = item.get("contract_type", "")

                    domains, roles = classify_job(title, desc)
                    stage_metadata = parse_internship_and_work_mode(title, desc, raw_contract)

                    # On garde l'offre si elle touche à la Data/IA OU s'il s'agit d'un stage Data
                    if domains:
                        if jid not in delta_jobs:
                            item["matched_domains"] = domains
                            item["matched_roles"] = roles
                            # Injection des métadonnées analytiques de stage & télétravail
                            item["analytics_metadata"] = stage_metadata
                            delta_jobs[jid] = item
                            seen_ids.add(jid)

            except Exception as e:
                logging.error(f"Erreur Adzuna ({term} - page {page}): {e}")

    new_records = list(delta_jobs.values())
    logging.info(f"[CDC] Nouvelles offres détectées (Delta) : {len(new_records)}")

    if not new_records:
        logging.info("[CDC] Aucun nouveau delta à ingérer. Arrêt propre sans écriture inutile.")
        return

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%H%M%S")
    object_key = f"api_adzuna/{date_partition}/adzuna_delta_{timestamp}.json"

    # Statistiques du delta pour les logs
    nb_stages = sum(1 for j in new_records if j.get("analytics_metadata", {}).get("is_internship"))

    payload = {
        "metadata": {
            "source": "adzuna_api",
            "country": country,
            "pipeline_stage": "bronze_delta",
            "ingestion_timestamp_utc": now.isoformat(),
            "new_records_count": len(new_records),
            "internships_count": nb_stages,
            "domains_covered": list(DATA_AI_TAXONOMY.keys())
        },
        "records": new_records
    }

    logging.info(f"Envoi du Delta vers MinIO ({nb_stages} stages identifiés) : s3://{BUCKET_NAME}/{object_key}...")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    save_seen_ids(s3, seen_ids)
    logging.info(f"[CDC] Ingestion terminée ! {len(new_records)} nouvelles offres stockées.")

if __name__ == "__main__":
    fetch_adzuna_delta(country="fr", max_pages_per_term=2)
