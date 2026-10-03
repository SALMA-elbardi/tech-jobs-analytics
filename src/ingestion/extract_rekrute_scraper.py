import os
import re
import json
import logging
import time
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Paramètres MinIO S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BRONZE_BUCKET", "bronze")

if not MINIO_USER or not MINIO_PASSWORD:
    raise ValueError("Identifiants MinIO manquants dans le fichier .env !")

STATE_KEY = "_state/rekrute_seen_urls.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"
}

# Cartographie exhaustive des 12 familles Data & IA
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

REKRUTE_SEARCH_KEYWORDS = [
    "data", "intelligence artificielle", "business intelligence",
    "power bi", "machine learning", "stage data", "pfe data"
]

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def load_seen_urls(s3):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return set(data.get("seen_urls", []))
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return set()
        raise e

def save_seen_urls(s3, seen_urls):
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_seen": len(seen_urls),
        "seen_urls": list(seen_urls)
    }
    s3.put_object(Bucket=BUCKET_NAME, Key=STATE_KEY, Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")

def parse_internship_and_work_mode(text):
    corpus = text.lower()
    is_internship = False
    internship_type = "CDI_OR_OTHER"

    if re.search(r"\b(pfe|projet de fin d'études|fin d'etude)\b", corpus):
        is_internship = True
        internship_type = "STAGE_PFE"
    elif re.search(r"\b(stage|internship|stagiaire)\b", corpus):
        is_internship = True
        internship_type = "STAGE"
    elif re.search(r"\b(alternance|apprentissage)\b", corpus):
        is_internship = True
        internship_type = "ALTERNANCE"

    duration_match = re.search(r"(\d+)\s*(?:à|-)?\s*(\d+)?\s*(?:mois|months)", corpus)
    internship_duration = duration_match.group(0) if duration_match else None

    is_remunerated = None
    if re.search(r"\b(rémunéré|remunere|gratification|indemnité|indemnite|stipend)\b", corpus):
        is_remunerated = True
    elif re.search(r"\b(non rémunéré|non remunere|bénévole|unpaid)\b", corpus):
        is_remunerated = False

    work_mode = "SUR_SITE"
    if re.search(r"\b(full remote|100% remote|100% télétravail|télétravail total)\b", corpus):
        work_mode = "FULL_REMOTE"
    elif re.search(r"\b(hybride|hybrid|télétravail partiel)\b", corpus):
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

def scrape_rekrute_jobs(max_pages_per_keyword=6):
    s3 = get_s3_client()
    seen_urls = load_seen_urls(s3)
    logging.info(f"[SCRAPER CDC] Registre chargé : {len(seen_urls)} URLs déjà stockées.")

    base_url = "https://www.rekrute.com"
    delta_jobs = {}

    for kw in REKRUTE_SEARCH_KEYWORDS:
        logging.info(f"[SCRAPER] Recherche sur ReKrute pour le mot-clé : '{kw}'...")
        for page in range(1, max_pages_per_keyword + 1):
            search_url = f"{base_url}/offres.html?s=1&p={page}&keyword={kw}"
            try:
                resp = requests.get(search_url, headers=HEADERS, timeout=15)
                if resp.status_code != 200:
                    logging.warning(f"Statut {resp.status_code} sur {search_url}")
                    continue

                soup = BeautifulSoup(resp.text, "lxml")
                job_cards = soup.select("li.post-id")

                if not job_cards:
                    break

                for card in job_cards:
                    link_tag = card.select_one("h2 a.titreJob") or card.select_one("a.titreJob")
                    if not link_tag:
                        continue

                    job_href = link_tag.get("href", "")
                    full_job_url = urljoin(base_url, job_href)

                    # Règle CDC : ignorer si l'URL est déjà vue
                    if not full_job_url or full_job_url in seen_urls:
                        continue

                    job_title = link_tag.get_text(strip=True)

                    # Extraction entreprise, localisation et résumé
                    company_tag = card.select_one("div.holder div.text-holder p")
                    company_name = company_tag.get_text(strip=True) if company_tag else "Non spécifié"

                    info_tags = card.select("div.holder div.info ul li")
                    info_text = " | ".join(tag.get_text(strip=True) for tag in info_tags)

                    desc_tag = card.select_one("div.holder div.info div.text")
                    snippet_desc = desc_tag.get_text(strip=True) if desc_tag else ""

                    combined_text = f"{job_title} {company_name} {info_text} {snippet_desc}"

                    domains, roles = classify_job(job_title, combined_text)
                    analytics_meta = parse_internship_and_work_mode(combined_text)

                    # Détection d'une composante Data/IA ou d'un stage Data
                    if domains or analytics_meta["is_internship"]:
                        job_record = {
                            "source": "rekrute_scraper",
                            "job_url": full_job_url,
                            "title": job_title,
                            "company": company_name,
                            "summary": snippet_desc,
                            "raw_metadata": info_text,
                            "country": "Maroc",
                            "scraped_at": datetime.now(timezone.utc).isoformat(),
                            "matched_domains": domains,
                            "matched_roles": roles,
                            "analytics_metadata": analytics_meta
                        }

                        delta_jobs[full_job_url] = job_record
                        seen_urls.add(full_job_url)

                time.sleep(1)  # Temporisation bienveillante entre requêtes

            except Exception as e:
                logging.error(f"Erreur scraping {search_url} : {e}")

    new_jobs = list(delta_jobs.values())
    logging.info(f"[SCRAPER CDC] Nouvelles annonces ReKrute découvertes : {len(new_jobs)}")

    if not new_jobs:
        logging.info("[SCRAPER CDC] Aucune nouvelle annonce. Aucun fichier Bronze généré.")
        return

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H%M%S")
    object_key = f"scraper_rekrute/{date_str}/rekrute_delta_{time_str}.json"

    nb_stages = sum(1 for j in new_jobs if j["analytics_metadata"]["is_internship"])

    payload = {
        "metadata": {
            "source": "rekrute_scraper",
            "country": "Maroc",
            "pipeline_stage": "bronze_delta",
            "ingestion_timestamp_utc": now.isoformat(),
            "new_records_count": len(new_jobs),
            "internships_count": nb_stages,
            "domains_covered": list(DATA_AI_TAXONOMY.keys())
        },
        "records": new_jobs
    }

    logging.info(f"Envoi vers MinIO : s3://{BUCKET_NAME}/{object_key} ({nb_stages} stages)...")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    save_seen_urls(s3, seen_urls)
    logging.info(f"[SCRAPER CDC] Ingestion réussie ! Registre mis à jour ({len(seen_urls)} URLs au total).")

if __name__ == "__main__":
    scrape_rekrute_jobs(max_pages_per_keyword=2)
