"""
dag_builder — Airflow DAG
Runs every 5 minutes inside the Airflow worker.
Reads every config JSON in dags/config/, picks the right template,
and writes the generated .py DAG file into dags/etl/<conn_id>/.

No extra container needed.
"""
import json
import logging
import os
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# ── Paths ─────────────────────────────────────────────────────────────────────
# File lives at  dags/custom_dags/dag_builder.py
# ROOT  →        repo root  (custom_dags → dags → root)
ROOT       = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", str(ROOT / "dags" / "config")))
ETL_DIR    = Path(os.getenv("ETL_DIR",    str(ROOT / "dags" / "etl")))
TMPL_DIR   = ROOT / "dags" / "templates"

TEMPLATES = {
    "db_pandas":   TMPL_DIR / "data_load.template",
    "db_spark":    TMPL_DIR / "spark_load.template",
    "file_pandas": TMPL_DIR / "file_load.template",
    "file_spark":  TMPL_DIR / "spark_file_load.template",
}

log = logging.getLogger("dag_builder")


# ── Helpers (mirror web_ui/app.py) ────────────────────────────────────────────

def _clean_id(value):
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value or "")


def _tmpl_for_config(config):
    use_spark = bool(config.get("spark"))
    is_file   = config.get("pipeline_type") == "file_based"
    if is_file:
        return TEMPLATES["file_spark"] if use_spark else TEMPLATES["file_pandas"]
    return TEMPLATES["db_spark"] if use_spark else TEMPLATES["db_pandas"]


def _out_path(config):
    dag_id         = config["dag_id"]
    use_spark      = bool(config.get("spark"))
    is_file        = config.get("pipeline_type") == "file_based"
    target_conn_id = _clean_id(
        config.get("target", {}).get("dev", {}).get("target_db_conn_id", "default")
    )
    if is_file:
        prefix = "spark_file" if use_spark else "file"
        bare   = re.sub(r"^(Spark_File_|File_)(csv|parquet)_", "", dag_id, flags=re.IGNORECASE) or dag_id
    else:
        prefix = "spark" if use_spark else "etl"
        bare   = re.sub(r"^(Spark_DB_|DB_)", "", dag_id, flags=re.IGNORECASE) or dag_id
    filename = "%s_%s_%s.py" % (prefix, target_conn_id, bare)
    return ETL_DIR / target_conn_id / filename


def _load_all_configs():
    configs = []
    for path in CONFIG_DIR.rglob("*.json"):
        try:
            configs.append(json.loads(path.read_text()))
        except Exception as exc:
            log.warning("Could not parse %s: %s", path, exc)
    return configs


def _generate_one(config):
    tmpl = _tmpl_for_config(config)
    if not tmpl.exists():
        log.warning("Template missing: %s — skipping %s", tmpl, config.get("dag_id"))
        return False
    out = _out_path(config)
    out.parent.mkdir(parents=True, exist_ok=True)
    content = tmpl.read_text().replace("<dag_name>", config["dag_id"].upper())
    out.write_text(content)
    log.info("Generated %s  <- %s", str(out.relative_to(ROOT)), tmpl.name)
    return True


@dag(
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["system", "dag_builder"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Regenerates ETL DAG .py files from templates + config JSONs",
)
def dag_builder():
    configs = _load_all_configs()
    groups = {}
    for cfg in configs:
        conn_id = _clean_id(cfg.get("target", {}).get("dev", {}).get("target_db_conn_id", "default"))
        groups.setdefault(conn_id, []).append(cfg)

    for conn_id, cfgs in groups.items():
        with TaskGroup(group_id=f"conn_{conn_id}") as tg:
            for cfg in cfgs:
                @task(task_id=f"build_{_clean_id(cfg['dag_id'])}")
                def build_one(config):
                    _generate_one(config)
                build_one.override(task_id=f"build_{_clean_id(cfg['dag_id'])}")(cfg)
        tg

dag = dag_builder()
