"""One-shot migration: add pipeline metadata columns to all existing DB-pipeline target tables."""
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql://cenfri_db_admin:y5j9p4aijrU2V6B9"
    "@cenfri-aws.cxc8802a0cus.af-south-1.rds.amazonaws.com:5432/cenfri_prod",
    pool_pre_ping=True, connect_args={"connect_timeout": 10}
)

COLS = [
    ("_pipeline_inserted_at", "TIMESTAMP"),
    ("_pipeline_run_id",      "TEXT"),
    ("_source_system",        "TEXT"),
    ("_loaded_at",            "TIMESTAMP"),
    ("_row_checksum",         "TEXT"),
]

with engine.connect() as conn:
    tables = conn.execute(text("""
        SELECT DISTINCT
            COALESCE(NULLIF(schema, ''), split_part(dag_id,'_',2)) AS tgt_schema,
            table_name
        FROM pipeline.etl_metrics
        WHERE source_type = 'db'
        ORDER BY 1, 2
    """)).fetchall()

print(f"Migrating {len(tables)} table(s)...")
for tgt_schema, table_name in tables:
    with engine.begin() as conn:
        for col_name, col_type in COLS:
            conn.execute(text(
                f"ALTER TABLE {tgt_schema}.{table_name} "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            ))
    print(f"  OK  {tgt_schema}.{table_name}")
print("Done.")
