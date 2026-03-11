#!/bin/bash

set -e

PG_CONF="/var/lib/postgresql/data/postgresql.conf"
HBA_CONF="/var/lib/postgresql/data/pg_hba.conf"
DB_NAME="airflow"
USER_NAME="${OPENMENTA_USER:-openmenta}"
PASSWORD="${OPENMENTA_PASSWORD:-changeme123}"

# Ensure listen_addresses is '*'
grep -q "^listen_addresses = '\*'" "$PG_CONF" || echo "listen_addresses = '*'" >> "$PG_CONF"

if grep -q "^shared_preload_libraries" "$PG_CONF"; then
    sed -i "s|^shared_preload_libraries.*|shared_preload_libraries = 'pg_stat_statements'|" "$PG_CONF"
else
    echo "shared_preload_libraries = 'pg_stat_statements'" >> "$PG_CONF"
fi
# Allow external connections
grep -q "^host\s\+all\s\+all\s\+0.0.0.0/0\s\+md5" "$HBA_CONF" || echo "host all all 0.0.0.0/0 md5" >> "$HBA_CONF"

# Create user if it doesn’t exist and grant access
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" <<-EOSQL
DO \$\$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = '${USER_NAME}'
   ) THEN
      CREATE USER ${USER_NAME} WITH PASSWORD '${PASSWORD}';
   END IF;
END
\$\$;

GRANT CONNECT ON DATABASE ${DB_NAME} TO ${USER_NAME};
\c ${DB_NAME}
GRANT USAGE ON SCHEMA public TO ${USER_NAME};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${USER_NAME};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${USER_NAME};

GRANT SELECT ON pg_stat_statements TO ${USER_NAME};


EOSQL
