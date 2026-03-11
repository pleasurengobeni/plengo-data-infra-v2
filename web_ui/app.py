import json
import os
import re
from pathlib import Path

from flask import Flask, redirect, render_template, request, url_for, session
from werkzeug.security import check_password_hash, generate_password_hash
from sqlalchemy import create_engine, text


APP_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", APP_ROOT / "dags" / "config"))
SQL_DIR = Path(os.getenv("SQL_DIR", APP_ROOT / "dags" / "sql"))
ETL_DIR = Path(os.getenv("ETL_DIR", APP_ROOT / "dags" / "etl"))
TEMPLATE_FILE            = Path(os.getenv("TEMPLATE_FILE",            APP_ROOT / "dags" / "templates" / "data_load.template"))
SPARK_TEMPLATE_FILE      = Path(os.getenv("SPARK_TEMPLATE_FILE",      APP_ROOT / "dags" / "templates" / "spark_load.template"))
FILE_TEMPLATE_FILE       = Path(os.getenv("FILE_TEMPLATE_FILE",       APP_ROOT / "dags" / "templates" / "file_load.template"))
SPARK_FILE_TEMPLATE_FILE = Path(os.getenv("SPARK_FILE_TEMPLATE_FILE", APP_ROOT / "dags" / "templates" / "spark_file_load.template"))
WEBUI_DB = os.getenv("WEBUI_DB", str(APP_ROOT / "web_ui" / "webui.db"))

# Mirror the Airflow Variable so the UI can highlight the active environment
ACTIVE_ENV = os.getenv("AIRFLOW_VAR_ENVIRONMENT", os.getenv("AIRFLOW_VAR_environment", "dev"))

AIRFLOW_DB = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN"),
)

app = Flask(__name__)
app.secret_key = os.getenv("WEBUI_SECRET_KEY", "plengo_webui_secret_key")
# Use a distinct cookie name so the Web UI session never collides with
# Airflow's own 'session' cookie (both run on the same browser/domain).
app.config["SESSION_COOKIE_NAME"] = os.getenv("WEBUI_SESSION_COOKIE_NAME", "plengo_webui_session")

SCHEDULE_SUGGESTIONS = [
    ("Every minute",        "* * * * *"),
    ("Every 5 minutes",     "*/5 * * * *"),
    ("Every 10 minutes",    "*/10 * * * *"),
    ("Every 30 minutes",    "*/30 * * * *"),
    ("Every hour",          "@hourly"),
    ("Every day at 2am",    "0 2 * * *"),
    ("Every day at midnight","0 0 * * *"),
    ("Every week (Mon 2am)","0 2 * * 1"),
    ("Every month (1st 2am)","0 2 1 * *"),
    ("Every year (Jan 1)",  "0 2 1 1 *"),
]


def clean_id(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value or "")


def _config_path(dag_id: str, target_conn_id: str = None) -> Path:
    """Return path to the JSON config file, nested under target_conn_id subfolder."""
    if target_conn_id:
        filename = f"{clean_id(target_conn_id)}_{dag_id.lower()}.json"
        return CONFIG_DIR / clean_id(target_conn_id) / filename
    # Fallback: search all subfolders
    for path in CONFIG_DIR.rglob(f"*_{dag_id.lower()}.json"):
        return path
    # Legacy fallback (plain dag_id.json)
    for path in CONFIG_DIR.rglob(f"{dag_id.lower()}.json"):
        return path
    return CONFIG_DIR / f"{dag_id.lower()}.json"


def _sql_dir(target_conn_id: str) -> Path:
    """Return SQL directory nested under target_conn_id subfolder."""
    return SQL_DIR / clean_id(target_conn_id) if target_conn_id else SQL_DIR


def _file_config_path(dag_id: str, target_conn_id: str = None) -> Path:
    """Return path to a FILE ETL JSON config — prefixed with 'file_' to avoid collisions."""
    # Strip auto-generated engine/format prefix so the filename always uses the bare DAG name.
    # e.g. 'Spark_File_csv_winequality_white' → 'winequality_white'
    bare = re.sub(r'^(Spark_File_|File_)(csv|parquet)_', '', dag_id, flags=re.IGNORECASE).lower()
    if target_conn_id:
        filename = f"file_{clean_id(target_conn_id)}_{bare}.json"
        return CONFIG_DIR / clean_id(target_conn_id) / filename
    for path in CONFIG_DIR.rglob(f"file_*_{bare}.json"):
        return path
    return CONFIG_DIR / f"file_{bare}.json"


def load_file_config_files():
    """Load only file-based ETL configs (prefixed with 'file_')."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    configs = []
    for path in sorted(CONFIG_DIR.rglob("file_*.json")):
        with path.open() as handle:
            configs.append(json.load(handle))
    return configs


def load_file_config(dag_id: str):
    path = _file_config_path(dag_id)
    if not path or not path.exists():
        return None
    with path.open() as handle:
        return json.load(handle)


def save_file_config(config: dict):
    target_conn_id = config.get("target", {}).get("dev", {}).get("target_db_conn_id", "")
    path = _file_config_path(config["dag_id"], target_conn_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        json.dump(config, handle, indent=4)
    generate_file_dag_py(config)


def delete_file_config(dag_id: str):
    path = _file_config_path(dag_id)
    if path and path.exists():
        path.unlink()
        try:
            path.parent.rmdir()
        except OSError:
            pass


def delete_file_dag_files(dag_id: str):
    """Delete file-ETL .py files (prefixed file_ or spark_file_)."""
    safe_dag = clean_id(dag_id)
    if not ETL_DIR.exists():
        return
    for pat in [f"file_*_{safe_dag}.py", f"spark_file_*_{safe_dag}.py"]:
        for found in ETL_DIR.rglob(pat):
            try:
                found.unlink()
            except OSError:
                pass
            try:
                found.parent.rmdir()
            except OSError:
                pass


def generate_file_dag_py(config: dict):
    """Generate the Airflow DAG .py for a file-based ETL job."""
    use_spark = bool(config.get("spark"))
    tmpl_file = SPARK_FILE_TEMPLATE_FILE if use_spark else FILE_TEMPLATE_FILE
    if not tmpl_file.exists():
        return
    target_conn_id = clean_id(
        config.get("target", {}).get("dev", {}).get("target_db_conn_id", "default")
    )
    dag_id   = config["dag_id"]
    prefix   = "spark_file" if use_spark else "file"
    # Strip auto-prefix so filename uses bare name: File_csv_winequality_red -> winequality_red
    bare     = re.sub(r'^(Spark_File_|File_)(csv|parquet)_', '', dag_id, flags=re.IGNORECASE) or dag_id
    filename = f"{prefix}_{target_conn_id}_{bare}.py"
    out_dir  = ETL_DIR / target_conn_id
    out_dir.mkdir(parents=True, exist_ok=True)
    content  = tmpl_file.read_text()
    content  = content.replace("<dag_name>", dag_id.upper())
    (out_dir / filename).write_text(content)


def build_file_config_from_form(form, apply_prod_same_as_dev: bool):
    """Build a file-based ETL config dict from a file_form.html POST."""

    def _parse_list(raw: str) -> list:
        return [v.strip() for v in (raw or "").split(",") if v.strip()]

    def src_block(prefix):
        raw_overrides = form.get(f"{prefix}_dtype_overrides_json", "") or "{}"
        try:
            dtype_overrides = json.loads(raw_overrides)
        except (ValueError, TypeError):
            dtype_overrides = {}
        watch_path = form.get(f"{prefix}_watch_path", "").strip() or "/opt/airflow/data_dump/incoming"
        file_name = form.get(f"{prefix}_file_name", "").strip()
        archive_path = form.get(f"{prefix}_archive_path", "").strip() or None
        return {
            "watch_path":             watch_path,
            "file_name":              file_name,
            "file_format":            form.get(f"{prefix}_file_format", "csv"),
            "delimiter":              form.get(f"{prefix}_delimiter", ",") or ",",
            "encoding":               form.get(f"{prefix}_encoding", "auto") or "auto",
            "has_header":             form.get(f"{prefix}_has_header") in ("on", "true", "1"),
            "skip_rows":              int(form.get(f"{prefix}_skip_rows") or 0),
            "date_columns":           _parse_list(form.get(f"{prefix}_date_columns", "")),
            "null_values":            _parse_list(form.get(f"{prefix}_null_values", "NULL,null,None,NA,N/A,#N/A")),
            "archive_path":           archive_path,
            "archive_retention_days": int(form.get(f"{prefix}_archive_retention_days") or 30),
            "dtype_overrides":        dtype_overrides,
        }

    def tgt_block(prefix):
        return {
            "target_db_conn_id": form.get(f"{prefix}_target_db_conn_id"),
            "db_type":           form.get(f"{prefix}_target_db_type", "postgresql"),
            "target_schema":     form.get(f"{prefix}_target_schema", "").strip(),
            "target_table":      form.get(f"{prefix}_target_table", "").strip(),
        }

    dev_src = src_block("dev")
    dev_tgt = tgt_block("dev")

    if apply_prod_same_as_dev:
        prod_src = dict(dev_src)
        prod_tgt = dict(dev_tgt)
    else:
        prod_src = src_block("prod")
        prod_tgt = tgt_block("prod")

    raw_tags   = form.get("extra_tags", "")
    extra_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    created_by = form.get("created_by", "")
    tags = list(dict.fromkeys([created_by] + extra_tags)) if created_by else extra_tags

    use_spark    = form.get("use_spark") == "on"
    has_existing = bool(form.get("_existing_spark_master"))
    spark_cfg = None
    if use_spark:
        spark_cfg = {
            "master":          form.get("spark_master")          or "local[*]",
            "app_name":        form.get("spark_app_name")        or form.get("dag_id") or "spark_file_etl",
            "driver_memory":   form.get("spark_driver_memory")   or "1g",
            "executor_memory": form.get("spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("spark_executor_cores")  or 2),
            "num_executors":   int(form.get("spark_num_executors")    or 2),
        }
    elif has_existing:
        spark_cfg = {
            "master":          form.get("_existing_spark_master")          or "local[*]",
            "app_name":        form.get("_existing_spark_app_name")        or form.get("dag_id") or "spark_file_etl",
            "driver_memory":   form.get("_existing_spark_driver_memory")   or "1g",
            "executor_memory": form.get("_existing_spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("_existing_spark_executor_cores") or 2),
            "num_executors":   int(form.get("_existing_spark_num_executors")   or 2),
        }

    # ── Auto-prefix dag_id: File_csv_ / File_parquet_ / Spark_File_csv_ / Spark_File_parquet_
    raw_dag_id   = (form.get("dag_id") or "").strip()
    dev_fmt      = dev_src.get("file_format", "csv").lower()
    prefix       = ("Spark_File_" if use_spark else "File_") + dev_fmt + "_"
    # Strip any existing prefix first (idempotent re-save)
    bare_dag_id  = re.sub(r'^(Spark_File_|File_)(csv|parquet)_', '', raw_dag_id, flags=re.IGNORECASE)
    dag_id_final = prefix + bare_dag_id if bare_dag_id else raw_dag_id

    config = {
        "dag_id":            dag_id_final,
        "pipeline_type":     "file_based",
        "schedule_interval": form.get("schedule_interval"),
        "start_date":        form.get("start_date") or "2023-01-01",
        "tags":              tags,
        "source":            {"dev": dev_src, "prod": prod_src},
        "target":            {"dev": dev_tgt, "prod": prod_tgt},
    }
    if spark_cfg:
        config["spark"] = spark_cfg
    return config


def load_config_files():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    configs = []
    for path in sorted(CONFIG_DIR.rglob("*.json")):
        if path.name.startswith("file_"):
            continue  # file-ETL configs handled by load_file_config_files()
        with path.open() as handle:
            configs.append(json.load(handle))
    return configs


def load_config(dag_id: str):
    path = _config_path(dag_id)
    if not path or not path.exists():
        return None
    with path.open() as handle:
        return json.load(handle)


def save_config(config: dict):
    target_conn_id = config.get("target", {}).get("dev", {}).get("target_db_conn_id", "")
    path = _config_path(config["dag_id"], target_conn_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        json.dump(config, handle, indent=4)
    generate_dag_py(config)


def delete_config(dag_id: str):
    path = _config_path(dag_id)
    if path and path.exists():
        path.unlink()
        # Remove empty subfolder
        try:
            path.parent.rmdir()
        except OSError:
            pass


def delete_dag_files(dag_id: str):
    """Delete all SQL, .py and config files associated with a dag_id across all subfolders."""
    safe_dag = clean_id(dag_id)
    for search_root in (SQL_DIR, ETL_DIR):
        if not search_root.exists():
            continue
        if search_root == SQL_DIR:
            pattern = f"*_{safe_dag}*.sql"
        else:
            # Match both prefixes: etl_<conn>_<dag>.py and spark_<conn>_<dag>.py
            patterns = [f"etl_*_{safe_dag}.py", f"spark_*_{safe_dag}.py"]
            for pat in patterns:
                for found in search_root.rglob(pat):
                    try:
                        found.unlink()
                    except OSError:
                        pass
                    try:
                        found.parent.rmdir()
                    except OSError:
                        pass
            continue
        for found in search_root.rglob(pattern):
            try:
                found.unlink()
            except OSError:
                pass
            try:
                found.parent.rmdir()
            except OSError:
                pass


def generate_dag_py(config: dict):
    """Generate the Airflow DAG .py file from the correct template into etl/<target_conn_id>/."""
    use_spark  = bool(config.get("spark"))
    tmpl_file  = SPARK_TEMPLATE_FILE if use_spark else TEMPLATE_FILE
    if not tmpl_file.exists():
        return
    target_conn_id = clean_id(
        config.get("target", {}).get("dev", {}).get("target_db_conn_id", "default")
    )
    dag_id   = config["dag_id"]
    prefix   = "spark" if use_spark else "etl"
    # Strip auto-prefix so filename uses bare name: DB_pipeline_invoices -> pipeline_invoices
    bare     = re.sub(r'^(Spark_DB_|DB_)', '', dag_id, flags=re.IGNORECASE) or dag_id
    filename = f"{prefix}_{target_conn_id}_{bare}.py"
    out_dir  = ETL_DIR / target_conn_id
    out_dir.mkdir(parents=True, exist_ok=True)
    content  = tmpl_file.read_text()
    content  = content.replace("<dag_name>", dag_id.upper())
    (out_dir / filename).write_text(content)



def read_sql_text(sql_file: str) -> str:
    if not sql_file:
        return ""
    # sql_file may be a plain filename or a relative path like conn_id/file.sql
    path = SQL_DIR / sql_file
    if path.exists():
        return path.read_text()
    # Fallback: search subfolders
    for found in SQL_DIR.rglob(Path(sql_file).name):
        return found.read_text()
    return ""


def persist_sql_from_form(form, dag_id: str, apply_prod_same_as_dev: bool, existing_config=None):
    dev_sql_text = (form.get("dev_sql_text") or "").strip()
    prod_sql_text = (form.get("prod_sql_text") or "").strip()

    # Determine target conn id to use as subfolder
    target_conn_id = clean_id(form.get("dev_target_db_conn_id") or "")
    sql_subdir = _sql_dir(target_conn_id)
    sql_subdir.mkdir(parents=True, exist_ok=True)

    safe_dag = clean_id(dag_id or "dag")

    def existing_sql_file(env: str) -> str:
        if not existing_config:
            return ""
        return existing_config.get("source", {}).get(env, {}).get("sql_file", "")

    # When prod same as dev — share a single SQL file
    if apply_prod_same_as_dev:
        filename = f"{target_conn_id}_{safe_dag}.sql"
        sql_file = f"{target_conn_id}/{filename}"
        if dev_sql_text:
            (sql_subdir / filename).write_text(dev_sql_text)
        return {"dev": sql_file, "prod": sql_file}

    # Separate dev/prod SQL files
    dev_filename = f"{target_conn_id}_{safe_dag}_dev.sql"
    prod_filename = f"{target_conn_id}_{safe_dag}_prod.sql"

    dev_sql_file = f"{target_conn_id}/{dev_filename}"
    prod_sql_file = f"{target_conn_id}/{prod_filename}"

    if dev_sql_text:
        (sql_subdir / dev_filename).write_text(dev_sql_text)
    if prod_sql_text:
        (sql_subdir / prod_filename).write_text(prod_sql_text)

    return {
        "dev": dev_sql_file,
        "prod": prod_sql_file,
    }


def airflow_connections():
    if not AIRFLOW_DB:
        return []
    try:
        engine = create_engine(AIRFLOW_DB)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT conn_id FROM connection ORDER BY conn_id")).fetchall()
        connections = [row[0] for row in rows]
        return connections
    except Exception:
        return []


def webui_engine():
    return create_engine(f"sqlite:///{WEBUI_DB}")


def init_webui_db():
    engine = webui_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS webui_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_admin INTEGER NOT NULL DEFAULT 0,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
        )

        admin_user = os.getenv("WEBUI_ADMIN_USER")
        admin_pass = os.getenv("WEBUI_ADMIN_PASS")
        if admin_user and admin_pass:
            existing = conn.execute(
                text("SELECT id FROM webui_users WHERE username = :u"),
                {"u": admin_user},
            ).fetchone()
            if not existing:
                conn.execute(
                    text(
                        """
                        INSERT INTO webui_users (username, password_hash, is_admin, is_active)
                        VALUES (:u, :p, 1, 1)
                        """
                    ),
                    {"u": admin_user, "p": generate_password_hash(admin_pass)},
                )


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    engine = webui_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, username, is_admin, is_active FROM webui_users WHERE id = :id"
            ),
            {"id": user_id},
        ).fetchone()
    return row


def require_login():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    return None


def require_admin():
    user = current_user()
    if not user or not user[2]:
        return redirect(url_for("jobs"))
    return None


def build_config_from_form(form, apply_prod_same_as_dev: bool):
    def env_block(prefix):
        return {
            "source_db_conn_id": form.get(f"{prefix}_source_db_conn_id"),
            "db_type": form.get(f"{prefix}_source_db_type"),
            "incremental_column": form.get(f"{prefix}_hwm_column"),
            "incremental_column_datatype": form.get(f"{prefix}_hwm_datatype"),
            "batch_size": int(form.get(f"{prefix}_batch_size") or 0),
            "full_load": form.get(f"{prefix}_full_load") == "on",
            "sql_file": None,  # populated by persist_sql_from_form after build
        }

    def target_block(prefix):
        return {
            "target_db_conn_id": form.get(f"{prefix}_target_db_conn_id"),
            "db_type": form.get(f"{prefix}_target_db_type"),
            "target_schema": form.get(f"{prefix}_target_schema"),
            "target_table": form.get(f"{prefix}_target_table"),
            "hwm_column": form.get(f"{prefix}_target_hwm_column"),
            "load_strategy": form.get(f"{prefix}_load_strategy"),
        }

    dev_source = env_block("dev")
    dev_target = target_block("dev")

    if apply_prod_same_as_dev:
        prod_source = dict(dev_source)
        prod_target = dict(dev_target)
    else:
        prod_source = env_block("prod")
        prod_target = target_block("prod")

    # Tags: always include the submitting user; merge with any extra tags typed
    raw_tags = form.get("extra_tags", "")
    extra_tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    created_by = form.get("created_by", "")
    tags = list(dict.fromkeys([created_by] + extra_tags)) if created_by else extra_tags

    use_spark     = form.get("use_spark") == "on"
    has_existing  = bool(form.get("_existing_spark_master"))  # set by hidden inputs on edit
    spark_cfg = None
    if use_spark:
        # User explicitly toggled Spark on — read live form fields
        spark_cfg = {
            "master":          form.get("spark_master")          or "local[*]",
            "app_name":        form.get("spark_app_name")        or form.get("dag_id") or "etl_spark",
            "driver_memory":   form.get("spark_driver_memory")   or "1g",
            "executor_memory": form.get("spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("spark_executor_cores")  or 2),
            "num_executors":   int(form.get("spark_num_executors")    or 2),
        }
    elif has_existing:
        # Spark was configured before; toggle not visible / not changed — preserve it
        spark_cfg = {
            "master":          form.get("_existing_spark_master")          or "local[*]",
            "app_name":        form.get("_existing_spark_app_name")        or form.get("dag_id") or "etl_spark",
            "driver_memory":   form.get("_existing_spark_driver_memory")   or "1g",
            "executor_memory": form.get("_existing_spark_executor_memory") or "2g",
            "executor_cores":  int(form.get("_existing_spark_executor_cores") or 2),
            "num_executors":   int(form.get("_existing_spark_num_executors")   or 2),
        }

    # ── Auto-prefix dag_id: Spark_DB_ / DB_ for DB-to-DB jobs ───────────────
    raw_dag_id  = (form.get("dag_id") or "").strip()
    db_prefix   = "Spark_DB_" if use_spark else "DB_"
    bare_dag_id = re.sub(r'^(Spark_DB_|DB_)', '', raw_dag_id, flags=re.IGNORECASE)
    dag_id      = db_prefix + bare_dag_id if bare_dag_id else raw_dag_id

    config = {
        "dag_id":           dag_id,
        "schedule_interval":form.get("schedule_interval"),
        "start_date":       form.get("start_date") or "2023-01-01",
        "batch_size":       int(form.get("batch_size") or 0),
        "read_chunk_size":  int(form.get("read_chunk_size") or 50000),
        "write_mode":       form.get("write_mode") or "append",
        "tags":             tags,
        "source":           {"dev": dev_source, "prod": prod_source},
        "target":           {"dev": dev_target, "prod": prod_target},
    }
    if spark_cfg:
        config["spark"] = spark_cfg
    return config


@app.route("/")
def index():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    configs = load_config_files()
    engine = webui_engine()
    with engine.connect() as conn:
        user_count = conn.execute(text("SELECT COUNT(*) FROM webui_users")).scalar()
    return render_template("index.html", user=current_user(), job_count=len(configs), user_count=user_count)


@app.route("/login", methods=["GET", "POST"])
def login():
    init_webui_db()
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        engine = webui_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, password_hash, is_active
                    FROM webui_users WHERE username = :u
                    """
                ),
                {"u": username},
            ).fetchone()
        if row and row[2] and check_password_hash(row[1], password):
            session["user_id"] = row[0]
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid credentials")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/profile", methods=["GET", "POST"])
def profile():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp

    user = current_user()
    if request.method == "POST":
        password = request.form.get("password")
        if password:
            engine = webui_engine()
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE webui_users SET password_hash = :p WHERE id = :id"),
                    {"p": generate_password_hash(password), "id": user[0]},
                )
        return redirect(url_for("index"))

    return render_template("profile.html", user=user)


@app.route("/jobs")
def jobs():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    configs = load_config_files()
    return render_template("jobs.html", jobs=configs, user=current_user(), pipeline_type="db")


@app.route("/jobs/new", methods=["GET", "POST"])
def new_job():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    if request.method == "POST":
        apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
        config = build_config_from_form(request.form, apply_prod_same_as_dev)
        sql_files = persist_sql_from_form(request.form, config["dag_id"], apply_prod_same_as_dev)
        config["source"]["dev"]["sql_file"] = sql_files["dev"]
        config["source"]["prod"]["sql_file"] = sql_files["prod"]
        save_config(config)
        return redirect(url_for("jobs"))

    _user = current_user()
    return render_template(
        "form.html",
        config=None,
        connections=airflow_connections(),
        dev_sql_text="",
        prod_sql_text="",
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=[],
        active_env=ACTIVE_ENV,
    )


@app.route("/jobs/<dag_id>/edit", methods=["GET", "POST"])
def edit_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    config = load_config(dag_id)
    if not config:
        return redirect(url_for("jobs"))

    if request.method == "POST":
        apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
        new_config = build_config_from_form(request.form, apply_prod_same_as_dev)
        sql_files = persist_sql_from_form(
            request.form,
            new_config["dag_id"],
            apply_prod_same_as_dev,
            existing_config=config,
        )
        new_config["source"]["dev"]["sql_file"] = sql_files["dev"]
        new_config["source"]["prod"]["sql_file"] = sql_files["prod"]
        save_config(new_config)
        return redirect(url_for("jobs"))

    _user = current_user()
    existing_tags = config.get("tags", [])
    # Extra tags = all tags except the owner username
    owner = existing_tags[0] if existing_tags else ""
    extra_tags = existing_tags[1:] if len(existing_tags) > 1 else []
    return render_template(
        "form.html",
        config=config,
        connections=airflow_connections(),
        dev_sql_text=read_sql_text(config.get("source", {}).get("dev", {}).get("sql_file")),
        prod_sql_text=read_sql_text(config.get("source", {}).get("prod", {}).get("sql_file")),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=existing_tags,
        extra_tags_str=", ".join(extra_tags),
        active_env=ACTIVE_ENV,
    )


@app.route("/jobs/<dag_id>/delete", methods=["POST"])
def delete_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    delete_config(dag_id)
    delete_dag_files(dag_id)
    return redirect(url_for("jobs"))


@app.route("/users")
def users():
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    engine = webui_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, username, is_admin, is_active FROM webui_users ORDER BY id")
        ).fetchall()
    return render_template("users.html", users=rows, user=current_user())


@app.route("/users/new", methods=["POST"])
def create_user():
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    username = request.form.get("username")
    password = request.form.get("password")
    is_admin = 1 if request.form.get("is_admin") == "on" else 0
    if username and password:
        engine = webui_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO webui_users (username, password_hash, is_admin, is_active)
                    VALUES (:u, :p, :a, 1)
                    """
                ),
                {"u": username, "p": generate_password_hash(password), "a": is_admin},
            )
    return redirect(url_for("users"))


@app.route("/users/<int:user_id>/toggle", methods=["POST"])
def toggle_user(user_id):
    redirect_resp = require_admin()
    if redirect_resp:
        return redirect_resp

    engine = webui_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE webui_users SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = :id"
            ),
            {"id": user_id},
        )
    return redirect(url_for("users"))


# ──────────────────────────────────────────────────────────────────────────────
# File-based ETL routes  (completely separate from DB-to-DB /jobs/* routes)
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/preview-columns", methods=["POST"])
def api_preview_columns():
    """
    Accept a file upload (or a server-side path) and return column names
    with suggested dtypes as JSON.  Used by the file_form wizard.
    """
    import io
    from flask import jsonify

    DTYPE_LABELS = {
        "int64":    "integer",
        "float64":  "decimal",
        "bool":     "boolean",
        "datetime": "timestamp",
        "object":   "text",
    }

    try:
        import pandas as pd
        import numpy as np

        file_obj = request.files.get("sample_file")
        file_format  = request.form.get("file_format", "csv").lower()
        delimiter    = request.form.get("delimiter", ",") or ","
        has_header   = request.form.get("has_header", "true").lower() != "false"
        skip_rows    = int(request.form.get("skip_rows", 0) or 0)
        encoding     = request.form.get("encoding", "utf-8") or "utf-8"
        null_values  = [v.strip() for v in request.form.get("null_values", "NULL,null,None,NA").split(",") if v.strip()]

        if not file_obj:
            return jsonify({"error": "No file uploaded"}), 400

        raw = file_obj.read()

        # Auto-detect delimiter if set to 'auto' or empty
        detected_delimiter = delimiter
        if file_format != "parquet" and (not delimiter or delimiter == "auto"):
            import csv as _csv
            try:
                sample_text = raw[:4096].decode(encoding if encoding not in ("", "auto") else "utf-8", errors="replace")
                sniffer = _csv.Sniffer()
                detected_delimiter = sniffer.sniff(sample_text, delimiters=",;|\t").delimiter
            except Exception:
                detected_delimiter = ","
            delimiter = detected_delimiter

        if file_format == "parquet":
            df = pd.read_parquet(io.BytesIO(raw))
        else:
            # Try the requested encoding, fall back to latin-1
            try:
                text_io = io.StringIO(raw.decode(encoding, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                text_io = io.StringIO(raw.decode("latin-1", errors="replace"))

            df = pd.read_csv(
                text_io,
                sep=delimiter,
                header=0 if has_header else None,
                skiprows=skip_rows if skip_rows else None,
                na_values=null_values,
                keep_default_na=True,
                nrows=500,      # sample only
                low_memory=False,
            )

        columns = []
        for col in df.columns:
            series = df[col]
            # Infer dtype label
            if pd.api.types.is_integer_dtype(series):
                dtype = "integer"
            elif pd.api.types.is_float_dtype(series):
                # Check if it looks like it should be integer (all values whole numbers)
                non_null = series.dropna()
                if len(non_null) > 0 and (non_null % 1 == 0).all():
                    dtype = "integer"
                else:
                    dtype = "decimal"
            elif pd.api.types.is_bool_dtype(series):
                dtype = "boolean"
            elif pd.api.types.is_datetime64_any_dtype(series):
                dtype = "timestamp"
            else:
                # Try to parse as number
                numeric = pd.to_numeric(series, errors="coerce")
                non_null = series.dropna()
                parsed   = numeric.dropna()
                if len(non_null) > 0 and len(parsed) / len(non_null) >= 0.8:
                    # Looks numeric — integer or decimal?
                    if (numeric.dropna() % 1 == 0).all():
                        dtype = "integer"
                    else:
                        dtype = "decimal"
                else:
                    # Try date
                    try:
                        parsed_dt = pd.to_datetime(series, errors="coerce", infer_datetime_format=True)
                        if len(non_null) > 0 and parsed_dt.notna().sum() / len(non_null) >= 0.8:
                            dtype = "timestamp"
                        else:
                            dtype = "text"
                    except Exception:
                        dtype = "text"

            # Sample values (first 3 non-null)
            sample = [str(v) for v in series.dropna().head(3).tolist()]
            # Normalise column name (same logic as template)
            import unicodedata
            clean = unicodedata.normalize("NFD", str(col))
            clean = "".join(c for c in clean if unicodedata.category(c) != "Mn")
            clean = re.sub(r"[^a-z0-9]+", "_", clean.lower()).strip("_") or "col"

            columns.append({
                "original": str(col),
                "clean":    clean,
                "dtype":    dtype,
                "sample":   sample,
            })

        return jsonify({"columns": columns, "detected_delimiter": detected_delimiter})

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/file-jobs")
def file_jobs():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    configs = load_file_config_files()
    return render_template("jobs.html", jobs=configs, user=current_user(), pipeline_type="file_based")


@app.route("/file-jobs/new", methods=["GET", "POST"])
def new_file_job():
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    _user = current_user()
    if request.method == "POST":
        try:
            apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
            config = build_file_config_from_form(request.form, apply_prod_same_as_dev)
            if not config.get("dag_id"):
                raise ValueError("DAG ID is required.")
            save_file_config(config)
            return redirect(url_for("file_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving file job")
            return render_template(
                "file_form.html",
                config=None,
                connections=airflow_connections(),
                schedule_suggestions=SCHEDULE_SUGGESTIONS,
                user=_user,
                current_username=_user[1] if _user else "",
                existing_tags=[],
                active_env=ACTIVE_ENV,
                save_error=str(exc),
                form_data=request.form,
            ), 422
    return render_template(
        "file_form.html",
        config=None,
        connections=airflow_connections(),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=[],
        active_env=ACTIVE_ENV,
    )


@app.route("/file-jobs/<dag_id>/edit", methods=["GET", "POST"])
def edit_file_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    config = load_file_config(dag_id)
    if not config:
        return redirect(url_for("file_jobs"))

    _user = current_user()
    existing_tags = config.get("tags", [])
    extra_tags = existing_tags[1:] if len(existing_tags) > 1 else []
    bare_dag_id = re.sub(r'^(Spark_File_|File_)(csv|parquet)_', '', config.get("dag_id", ""), flags=re.IGNORECASE)
    if request.method == "POST":
        try:
            apply_prod_same_as_dev = request.form.get("apply_prod_same_as_dev") == "on"
            new_config = build_file_config_from_form(request.form, apply_prod_same_as_dev)
            save_file_config(new_config)
            return redirect(url_for("file_jobs"))
        except Exception as exc:
            app.logger.exception("Error saving file job")
            return render_template(
                "file_form.html",
                config=config,
                connections=airflow_connections(),
                schedule_suggestions=SCHEDULE_SUGGESTIONS,
                user=_user,
                current_username=_user[1] if _user else "",
                existing_tags=existing_tags,
                extra_tags_str=", ".join(extra_tags),
                active_env=ACTIVE_ENV,
                save_error=str(exc),
                form_data=request.form,
                bare_dag_id=bare_dag_id,
            ), 422
    return render_template(
        "file_form.html",
        config=config,
        connections=airflow_connections(),
        schedule_suggestions=SCHEDULE_SUGGESTIONS,
        user=_user,
        current_username=_user[1] if _user else "",
        existing_tags=existing_tags,
        extra_tags_str=", ".join(extra_tags),
        active_env=ACTIVE_ENV,
        bare_dag_id=bare_dag_id,
    )


@app.route("/file-jobs/<dag_id>/delete", methods=["POST"])
def delete_file_job(dag_id):
    redirect_resp = require_login()
    if redirect_resp:
        return redirect_resp
    delete_file_config(dag_id)
    delete_file_dag_files(dag_id)
    return redirect(url_for("file_jobs"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")), debug=True)
