import os
import json
import urllib.request
from datetime import datetime, timezone

def send_pipeline_alert(task_name, status, message, error_details=None):
    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    payload = {
        "content": f"**[PIPELINE NOTIFICATION - {status.upper()}]**",
        "embeds": [
            {
                "title": f"Tâche : {task_name}",
                "color": 15158332 if status.lower() == "failure" else 3066993,
                "fields": [
                    {"name": "Statut", "value": status, "inline": True},
                    {"name": "Date / Heure", "value": timestamp, "inline": True},
                    {"name": "Message", "value": message, "inline": False}
                ]
            }
        ]
    }
    
    if error_details:
        payload["embeds"][0]["fields"].append(
            {"name": "Détails Techniques", "value": f"```{str(error_details)[:500]}```", "inline": False}
        )

    # Si aucun Webhook n'est configuré dans .env, journalisation console locale
    if not webhook_url:
        print(f"[ALERTE SIMULÉE - {status.upper()}] Tâche: {task_name} | {message}")
        return

    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "TechJobsPipeline/1.0"}
        )
        urllib.request.urlopen(req, timeout=5)
        print(f"[ALERTE] Notification transmise avec succès pour {task_name}.")
    except Exception as e:
        print(f"[ALERTE ERREUR] Échec d'envoi du webhook : {e}")

def airflow_failure_callback(context):
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "Inconnue"
    exception = context.get("exception", "Aucun détail d'exception fourni")
    
    send_pipeline_alert(
        task_name=task_id,
        status="Failure",
        message="Une tâche du DAG a échoué pendant son exécution.",
        error_details=exception
    )