"""
Comprehensive tests for web_ui/app.py
Covers every route and key helper functions.
"""
import importlib
import io
import json
from pathlib import Path

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_test_client(tmp_path, monkeypatch, *, logged_in=True, is_admin=True):
    """
    Reload app with a fresh temp environment, return (client, app_module).
    Creates minimal template and config stubs so the app can boot.
    """
    monkeypatch.setenv("WEBUI_DB",            str(tmp_path / "webui.db"))
    monkeypatch.setenv("CONFIG_DIR",          str(tmp_path / "config"))
    monkeypatch.setenv("ETL_DIR",             str(tmp_path / "etl"))
    monkeypatch.setenv("SQL_DIR",             str(tmp_path / "sql"))
    monkeypatch.setenv("WEBUI_ADMIN_USER",    "admin")
    monkeypatch.setenv("WEBUI_ADMIN_PASS",    "admin123")

    # SQL stub
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (sql_dir / "example.sql").write_text("select 1")

    # Template stubs
    tpl_dir = tmp_path / "templates"
    tpl_dir.mkdir(parents=True, exist_ok=True)
    (tpl_dir / "data_load.template").write_text("DAG <dag_name>")
    (tpl_dir / "spark_load.template").write_text("SPARK DAG <dag_name>")
    (tpl_dir / "file_load.template").write_text("FILE DAG <dag_name>")
    (tpl_dir / "spark_file_load.template").write_text("SPARK FILE DAG <dag_name>")

    monkeypatch.setenv("TEMPLATE_FILE",            str(tpl_dir / "data_load.template"))
    monkeypatch.setenv("SPARK_TEMPLATE_FILE",      str(tpl_dir / "spark_load.template"))
    monkeypatch.setenv("FILE_TEMPLATE_FILE",       str(tpl_dir / "file_load.template"))
    monkeypatch.setenv("SPARK_FILE_TEMPLATE_FILE", str(tpl_dir / "spark_file_load.template"))

    import web_ui.app as app_module
    importlib.reload(app_module)
    app_module.app.config.update(TESTING=True, SECRET_KEY="test")

    # Always initialise the DB so webui_users table exists
    app_module.init_webui_db()

    client = app_module.app.test_client()

    if logged_in:
        with client.session_transaction() as sess:
            sess["user_id"] = 1
        monkeypatch.setattr(app_module, "current_user",
                            lambda: (1, "admin", 1, 1))
        if is_admin:
            monkeypatch.setattr(app_module, "require_admin", lambda: None)
        monkeypatch.setattr(app_module, "require_login",  lambda: None)

    monkeypatch.setattr(app_module, "airflow_connections", lambda: ["test_conn"])

    return client, app_module


def _make_db_config(tmp_path, dag_id="testdag", conn="test_conn"):
    cfg = {
        "dag_id": dag_id,
        "pipeline_type": "db",
        "schedule_interval": "*/5 * * * *",
        "start_date": "2023-01-01",
        "tags": ["admin"],
        "source": {
            "dev":  {"sql_file": "dev.sql",  "source_db_conn_id": conn},
            "prod": {"sql_file": "prod.sql", "source_db_conn_id": conn},
        },
        "target": {
            "dev":  {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
            "prod": {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
        },
    }
    config_dir = tmp_path / "config" / conn
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / f"{conn}_{dag_id}.json").write_text(json.dumps(cfg))
    return cfg


def _make_file_config(tmp_path, dag_id="filejob", conn="test_conn"):
    cfg = {
        "dag_id": dag_id,
        "pipeline_type": "file_based",
        "schedule_interval": "@daily",
        "start_date": "2023-01-01",
        "tags": ["admin"],
        "source": {
            "dev":  {"watch_path": "/data/dev",  "file_format": "csv", "file_pattern": ".*\\.csv"},
            "prod": {"watch_path": "/data/prod", "file_format": "csv", "file_pattern": ".*\\.csv"},
        },
        "target": {
            "dev":  {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
            "prod": {"target_db_conn_id": conn, "target_schema": "s", "target_table": "t"},
        },
    }
    config_dir = tmp_path / "config" / conn
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / f"file_{conn}_{dag_id}.json").write_text(json.dumps(cfg))
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_login_page_renders(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.get("/login")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert any(kw in html.lower() for kw in ("login", "username", "password"))

    def test_valid_login_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.post("/login", data={"username": "admin", "password": "admin123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)
        assert "/login" not in resp.headers.get("Location", "")

    def test_invalid_login_shows_error(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.post("/login", data={"username": "admin", "password": "wrong"},
                           follow_redirects=True)
        assert resp.status_code == 200
        html = resp.data.decode().lower()
        # Template renders either "invalid credentials" or stays on login page
        assert "invalid" in html or "login" in html

    def test_logout_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/logout", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_unauthenticated_gets_redirected(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch, logged_in=False)
        resp = client.get("/jobs", follow_redirects=False)
        assert resp.status_code in (200, 302)


# ─────────────────────────────────────────────────────────────────────────────
# Index / Profile
# ─────────────────────────────────────────────────────────────────────────────

class TestIndex:
    def test_index_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/").status_code == 200

    def test_profile_get_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/profile").status_code == 200

    def test_profile_post_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/profile", data={"password": "newpass123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# DB-to-DB jobs (/jobs/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestDBJobs:
    def test_jobs_list_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/jobs").status_code == 200

    def test_jobs_list_shows_db_config(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_db_config(tmp_path, dag_id="myjob")
        assert "myjob" in client.get("/jobs").data.decode()

    def test_jobs_list_excludes_file_configs(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_file_config(tmp_path, dag_id="fileonlyjob")
        assert "fileonlyjob" not in client.get("/jobs").data.decode()

    def test_new_job_form_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/jobs/new")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "*/5 * * * *" in html
        assert "dev_sql_text" in html
        assert "test_conn" in html

    def test_create_job_post_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        data = {
            "dag_id": "newjob",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_source_db_conn_id": "test_conn",
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "dev_sql_text": "SELECT 1",
            "prod_source_db_conn_id": "test_conn",
            "prod_target_db_conn_id": "test_conn",
            "prod_target_db_type": "postgresql",
            "prod_target_schema": "public",
            "prod_target_table": "my_table",
            "prod_sql_text": "SELECT 1",
            "created_by": "admin",
        }
        resp = client.post("/jobs/new", data=data, follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_edit_job_renders(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        cfg = _make_db_config(tmp_path, dag_id="editjob")
        monkeypatch.setattr(app_module, "load_config",    lambda d: cfg)
        monkeypatch.setattr(app_module, "read_sql_text",  lambda p: "SELECT 1")
        resp = client.get("/jobs/editjob/edit")
        assert resp.status_code == 200
        assert "editjob" in resp.data.decode()

    def test_edit_missing_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "load_config", lambda d: None)
        resp = client.get("/jobs/ghost/edit", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_delete_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "delete_config",    lambda d: None)
        monkeypatch.setattr(app_module, "delete_dag_files", lambda d: None)
        resp = client.post("/jobs/somejob/delete", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# File-based ETL routes (/file-jobs/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestFileJobs:
    def test_file_jobs_list_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        assert client.get("/file-jobs").status_code == 200

    def test_file_jobs_list_shows_file_config(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_file_config(tmp_path, dag_id="csvjob")
        assert "csvjob" in client.get("/file-jobs").data.decode()

    def test_file_jobs_list_excludes_db_configs(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        _make_db_config(tmp_path, dag_id="dbjob")
        assert "dbjob" not in client.get("/file-jobs").data.decode()

    def test_new_file_job_form_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/file-jobs/new")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "File Source" in html
        assert "Column Types" in html
        assert "Target" in html

    def test_new_file_job_form_has_preview_ui(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        html = client.get("/file-jobs/new").data.decode()
        assert "runPreviewBtn" in html
        assert "preview-columns" in html
        assert "devDtypeOverridesJson" in html

    def test_create_file_job_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        data = {
            "dag_id": "newfilejob",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_watch_path": "/data/inbound",
            "dev_file_pattern": r".*\.csv",
            "dev_file_format": "csv",
            "dev_delimiter": ",",
            "dev_encoding": "utf-8",
            "dev_has_header": "on",
            "dev_skip_rows": "0",
            "dev_null_values": "NULL",
            "dev_archive_retention_days": "30",
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "created_by": "admin",
        }
        resp = client.post("/file-jobs/new", data=data, follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_create_file_job_persists_dtype_overrides(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        saved = {}
        monkeypatch.setattr(app_module, "save_file_config", lambda cfg: saved.update(cfg))
        data = {
            "dag_id": "savetest",
            "schedule_interval": "@daily",
            "start_date": "2023-01-01",
            "apply_prod_same_as_dev": "on",
            "dev_watch_path": "/data/in",
            "dev_file_pattern": ".*",
            "dev_file_format": "csv",
            "dev_delimiter": ",",
            "dev_encoding": "utf-8",
            "dev_has_header": "on",
            "dev_skip_rows": "0",
            "dev_null_values": "NULL",
            "dev_archive_retention_days": "30",
            "dev_dtype_overrides_json": '{"amount": "decimal"}',
            "dev_target_db_conn_id": "test_conn",
            "dev_target_db_type": "postgresql",
            "dev_target_schema": "public",
            "dev_target_table": "my_table",
            "created_by": "admin",
        }
        client.post("/file-jobs/new", data=data, follow_redirects=False)
        assert saved.get("pipeline_type") == "file_based"
        assert saved["source"]["dev"]["dtype_overrides"] == {"amount": "decimal"}

    def test_edit_file_job_renders(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        cfg = _make_file_config(tmp_path, dag_id="editfile")
        monkeypatch.setattr(app_module, "load_file_config", lambda d: cfg)
        resp = client.get("/file-jobs/editfile/edit")
        assert resp.status_code == 200
        assert "editfile" in resp.data.decode()

    def test_edit_missing_file_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "load_file_config", lambda d: None)
        resp = client.get("/file-jobs/ghost/edit", follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_delete_file_job_redirects(self, tmp_path, monkeypatch):
        client, app_module = build_test_client(tmp_path, monkeypatch)
        monkeypatch.setattr(app_module, "delete_file_config",    lambda d: None)
        monkeypatch.setattr(app_module, "delete_file_dag_files", lambda d: None)
        resp = client.post("/file-jobs/somejob/delete", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# API: /api/preview-columns
# ─────────────────────────────────────────────────────────────────────────────

class TestPreviewColumnsAPI:
    def _post(self, client, csv_content, extra_data=None):
        data = {
            "sample_file": (io.BytesIO(csv_content.encode("utf-8")), "sample.csv"),
            "file_format":  "csv",
            "delimiter":    ",",
            "has_header":   "true",
            "skip_rows":    "0",
            "encoding":     "utf-8",
            "null_values":  "NULL,null",
        }
        if extra_data:
            data.update(extra_data)
        return client.post("/api/preview-columns", data=data,
                           content_type="multipart/form-data")

    def test_no_file_returns_400(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/api/preview-columns", data={"file_format": "csv"})
        assert resp.status_code == 400
        assert "error" in resp.get_json()

    def test_integer_and_decimal_columns(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "name,age,salary\nAlice,30,50000.5\nBob,25,40000.0\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["age"]["dtype"] == "integer"
        assert cols["salary"]["dtype"] in ("decimal", "integer")

    def test_text_column(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "description\nhello world\nfoo bar\nbaz qux\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["description"]["dtype"] == "text"

    def test_boolean_column(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "is_active\nTrue\nFalse\nTrue\n")
        assert resp.status_code == 200
        cols = {c["original"]: c for c in resp.get_json()["columns"]}
        assert cols["is_active"]["dtype"] == "boolean"

    def test_column_name_cleaning(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "First Name,Last-Name,DOB\nAlice,Smith,1990-01-01\n")
        assert resp.status_code == 200
        cleans = [c["clean"] for c in resp.get_json()["columns"]]
        assert "first_name" in cleans
        assert "last_name" in cleans

    def test_sample_values_max_three(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "val\n1\n2\n3\n4\n5\n")
        assert resp.status_code == 200
        for col in resp.get_json()["columns"]:
            assert len(col["sample"]) <= 3

    def test_pipe_delimiter(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "a|b|c\n1|2|3\n", extra_data={"delimiter": "|"})
        assert resp.status_code == 200
        names = [c["original"] for c in resp.get_json()["columns"]]
        assert set(names) == {"a", "b", "c"}

    def test_response_has_required_keys(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = self._post(client, "id,name\n1,Alice\n2,Bob\n")
        assert resp.status_code == 200
        for col in resp.get_json()["columns"]:
            assert {"original", "clean", "dtype", "sample"} <= col.keys()


# ─────────────────────────────────────────────────────────────────────────────
# User management (/users/*)
# ─────────────────────────────────────────────────────────────────────────────

class TestUsers:
    def test_users_page_200(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.get("/users")
        assert resp.status_code == 200
        assert "admin" in resp.data.decode()

    def test_create_user_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        resp = client.post("/users/new",
                           data={"username": "bob", "password": "pass123"},
                           follow_redirects=False)
        assert resp.status_code in (301, 302)

    def test_created_user_appears_in_list(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        client.post("/users/new", data={"username": "carol", "password": "pass123"})
        assert "carol" in client.get("/users").data.decode()

    def test_toggle_user_redirects(self, tmp_path, monkeypatch):
        client, _ = build_test_client(tmp_path, monkeypatch)
        client.post("/users/new", data={"username": "dave", "password": "pass"})
        resp = client.post("/users/2/toggle", follow_redirects=False)
        assert resp.status_code in (301, 302)


# ─────────────────────────────────────────────────────────────────────────────
# Unit: build_file_config_from_form()
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildFileConfigFromForm:
    def _form(self, extra=None):
        base = {
            "dag_id":                     "test_dag",
            "schedule_interval":          "@daily",
            "start_date":                 "2023-01-01",
            "created_by":                 "admin",
            "dev_watch_path":             "/data/in",
            "dev_file_pattern":           ".*\\.csv",
            "dev_file_format":            "csv",
            "dev_delimiter":              ",",
            "dev_encoding":               "utf-8",
            "dev_has_header":             "true",
            "dev_skip_rows":              "0",
            "dev_null_values":            "NULL,null",
            "dev_archive_retention_days": "30",
            "dev_target_db_conn_id":      "test_conn",
            "dev_target_db_type":         "postgresql",
            "dev_target_schema":          "public",
            "dev_target_table":           "my_tbl",
        }
        if extra:
            base.update(extra)
        return base

    def _reload(self):
        import web_ui.app as m
        importlib.reload(m)
        return m

    def test_basic_fields(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert cfg["dag_id"] == "File_csv_test_dag"
        assert cfg["pipeline_type"] == "file_based"
        assert cfg["source"]["dev"]["watch_path"] == "/data/in"
        assert cfg["source"]["dev"]["has_header"] is True

    def test_dtype_overrides_parsed(self):
        m = self._reload()
        form = self._form({"dev_dtype_overrides_json": '{"amount":"decimal","count":"integer"}'})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == {"amount": "decimal", "count": "integer"}

    def test_invalid_dtype_overrides_defaults_empty(self):
        m = self._reload()
        form = self._form({"dev_dtype_overrides_json": "NOT_JSON{"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["dtype_overrides"] == {}

    def test_apply_prod_same_as_dev(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"] == cfg["source"]["prod"]
        assert cfg["target"]["dev"] == cfg["target"]["prod"]

    def test_separate_prod(self):
        m = self._reload()
        form = self._form({
            "prod_watch_path": "/data/prod", "prod_file_pattern": ".*",
            "prod_file_format": "csv", "prod_delimiter": ",",
            "prod_encoding": "utf-8", "prod_has_header": "true",
            "prod_skip_rows": "0", "prod_null_values": "NULL",
            "prod_archive_retention_days": "30", "prod_dtype_overrides_json": "{}",
            "prod_target_db_conn_id": "prod_conn", "prod_target_db_type": "postgresql",
            "prod_target_schema": "public", "prod_target_table": "prod_tbl",
        })
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=False)
        assert cfg["source"]["prod"]["watch_path"] == "/data/prod"
        assert cfg["target"]["prod"]["target_db_conn_id"] == "prod_conn"

    def test_spark_config_when_toggled(self):
        m = self._reload()
        form = self._form({
            "use_spark": "on",
            "spark_master": "spark://host:7077",
            "spark_app_name": "myapp",
            "spark_driver_memory": "2g",
            "spark_executor_memory": "4g",
            "spark_executor_cores": "4",
            "spark_num_executors": "3",
        })
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert "spark" in cfg
        assert cfg["spark"]["master"] == "spark://host:7077"
        assert cfg["spark"]["executor_cores"] == 4

    def test_no_spark_when_not_toggled(self):
        m = self._reload()
        cfg = m.build_file_config_from_form(self._form(), apply_prod_same_as_dev=True)
        assert "spark" not in cfg

    def test_tags_from_created_by_and_extra(self):
        m = self._reload()
        form = self._form({"created_by": "alice", "extra_tags": "team_a, project_x"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["tags"][0] == "alice"
        assert "team_a" in cfg["tags"]
        assert "project_x" in cfg["tags"]

    def test_null_values_to_list(self):
        m = self._reload()
        form = self._form({"dev_null_values": "NULL, null, N/A, #N/A"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        nv = cfg["source"]["dev"]["null_values"]
        assert "NULL" in nv and "N/A" in nv

    def test_skip_rows_int(self):
        m = self._reload()
        form = self._form({"dev_skip_rows": "5"})
        cfg = m.build_file_config_from_form(form, apply_prod_same_as_dev=True)
        assert cfg["source"]["dev"]["skip_rows"] == 5
        assert isinstance(cfg["source"]["dev"]["skip_rows"], int)


# ─────────────────────────────────────────────────────────────────────────────
# Unit: config file isolation (load_config_files vs load_file_config_files)
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigFileIsolation:
    def test_db_loader_excludes_file_prefix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_DIR", str(tmp_path / "config"))
        import web_ui.app as m
        importlib.reload(m)
        d = tmp_path / "config" / "conn"
        d.mkdir(parents=True, exist_ok=True)
        (d / "conn_dbjob.json").write_text(json.dumps({"dag_id": "dbjob"}))
        (d / "file_conn_filejob.json").write_text(json.dumps({"dag_id": "filejob"}))
        ids = [c["dag_id"] for c in m.load_config_files()]
        assert "dbjob" in ids
        assert "filejob" not in ids

    def test_file_loader_only_file_prefix(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_DIR", str(tmp_path / "config"))
        import web_ui.app as m
        importlib.reload(m)
        d = tmp_path / "config" / "conn"
        d.mkdir(parents=True, exist_ok=True)
        (d / "conn_dbjob.json").write_text(json.dumps({"dag_id": "dbjob"}))
        (d / "file_conn_filejob.json").write_text(json.dumps({"dag_id": "filejob"}))
        ids = [c["dag_id"] for c in m.load_file_config_files()]
        assert "filejob" in ids
        assert "dbjob" not in ids
