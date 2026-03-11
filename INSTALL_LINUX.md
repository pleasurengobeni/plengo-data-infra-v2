# Linux Installation Guide — plengo_data-infra-v2

Step-by-step guide to install and run the full ETL stack on a fresh **Ubuntu 24.04 LTS** server.

---

## What you'll get

| Service | URL | Description |
|---|---|---|
| Airflow Webserver | http://\<server-ip\>:8090 | ETL pipeline orchestration |
| Web UI (ETL Manager) | http://\<server-ip\>:5001 | No-code pipeline config editor |
| Pipeline Monitor | http://\<server-ip\>:8050 | Real-time ETL metrics dashboard |
| Jenkins | http://\<server-ip\>:9090 | Auto-deploy on every git push |
| PgAdmin | http://\<server-ip\>:5050 | Web-based Postgres query UI |
| PostgreSQL | \<server-ip\>:55432 | Airflow metadata database |

---

## Requirements

- Ubuntu 22.04 or 24.04 LTS (64-bit)
- 4 GB RAM minimum (8 GB recommended)
- 2 CPU cores minimum
- 20 GB disk space
- Internet access (to pull Docker images and JDBC drivers)
- GitHub account with access to `pleasurengobeni/plengo_data-infra-v2`

---

## Step 1 — Create the project directory

```bash
cd ~/
mkdir -p datapipeline/airflow
cd datapipeline/airflow
```

---

## Step 2 — Install Docker

```bash
# Remove any old Docker installations
sudo apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true
sudo apt-get purge -y docker docker-engine docker.io containerd runc 2>/dev/null || true
sudo rm -rf /var/lib/docker /var/lib/containerd 2>/dev/null || true

# Update system
sudo apt-get update -y && sudo apt-get upgrade -y

# Install prerequisites
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Enable Docker service
sudo systemctl enable docker
sudo systemctl start docker

# Add your user to the docker group (avoids needing sudo for docker commands)
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker run hello-world
docker compose version
```

---

## Step 3 — Set up SSH access to GitHub

```bash
# Generate a new SSH key (press Enter at all prompts to accept defaults)
ssh-keygen -t ed25519 -C "your@email.com"

# Start the SSH agent and add your key
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519

# Print the public key — copy this output
cat ~/.ssh/id_ed25519.pub
```

Go to **GitHub → Settings → SSH and GPG keys → New SSH key**, paste the key, save.

```bash
# Test the connection — expect: "Hi username! You've successfully authenticated..."
ssh -T git@github.com
```

---

## Step 4 — Clone the repository

```bash
# Make sure you are inside ~/datapipeline/airflow
cd ~/datapipeline/airflow
git clone git@github.com:pleasurengobeni/plengo_data-infra-v2.git .
```

---

## Step 5 — Configure environment variables

All secrets are kept in `~/.airflow` on the server — never in the repo.

```bash
nano ~/.airflow
```

Paste and fill in your real values:

```bash
# ── Airflow metadata DB ───────────────────────────────────────────────────────
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"

# ── External data lake / target DB ───────────────────────────────────────────
# Used by ETL DAGs to write data; NOT used by the Pipeline Monitor dashboard.
POSTGRES_DATA_USER="your_db_user"
POSTGRES_DATA_PWD="your_db_password"
POSTGRES_DATA_HOST="your_db_host_or_ip"
POSTGRES_DATA_PORT=5432
POSTGRES_DATA_DB="your_db_name"

# ── Pipeline Monitor dashboard DB (where pipeline.etl_metrics lives) ─────────
# Set these only if the metrics table is on a DIFFERENT host than POSTGRES_DATA_*
# If left unset, the dashboard will use the defaults coded in dashboard.py.
# METRICS_DB_USER="cenfri_db_admin"
# METRICS_DB_PASS="your_password"
# METRICS_DB_HOST="your-rds-host.rds.amazonaws.com"
# METRICS_DB_PORT=5432
# METRICS_DB_NAME="your_db_name"

# ── PgAdmin ───────────────────────────────────────────────────────────────────
PGADMIN_DEFAULT_EMAIL="you@example.com"
PGADMIN_DEFAULT_PASSWORD="your_pgadmin_password"
PGADMIN_PORT=5050

# ── Airflow core ──────────────────────────────────────────────────────────────
AIRFLOW_UID=1000
AIRFLOW_GID=1000
DOCKER_AIRFLOW_HOME="/opt/airflow"
AIRFLOW__CORE__DEFAULT_TIMEZONE="Africa/Johannesburg"
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=1000
AIRFLOW__CORE__DAG_FILE_PROCESSOR_TIMEOUT=1000
AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@postgres/airflow"
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@postgres/airflow"
AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow:airflow@postgres/airflow"
AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=1000
AIRFLOW__WEBSERVER__WARN_DEPLOYMENT_EXPOSURE=False
AIRFLOW__SCHEDULER__PARSING_PROCESSES=4

# ── Security keys (generate unique values — see commands below) ───────────────
AIRFLOW__CORE__FERNET_KEY="your_fernet_key"
AIRFLOW__WEBSERVER__SECRET_KEY="your_airflow_secret_key"
WEBUI_SECRET_KEY="your_webui_secret_key"

# ── Airflow admin user (created on first start) ───────────────────────────────
_AIRFLOW_WWW_USER_USERNAME="admin"
_AIRFLOW_WWW_USER_PASSWORD="your_airflow_admin_password"

# ── Web UI admin ───────────────────────────────────────────────────────────────
WEBUI_ADMIN_USER="admin"
WEBUI_ADMIN_PASS="your_webui_password"

# ── Airflow Variables (loaded into Airflow on first run) ──────────────────────
AIRFLOW_VAR_ENVIRONMENT="prod"
AIRFLOW_VAR_MODULES_PATH="/opt/airflow/dags/modules"
AIRFLOW_VAR_CONFIG_PATH="/opt/airflow/dags/config"
AIRFLOW_VAR_SQL_PATH="/opt/airflow/dags/sql"
AIRFLOW_VAR_ETL_PATH="/opt/airflow/dags/etl"
AIRFLOW_VAR_TEMPLATE_PATH="/opt/airflow/dags/templates"
AIRFLOW_VAR_DATA_DUMP="/opt/airflow/data_dump"
AIRFLOW_VAR_LOGS_PATH="/opt/airflow/logs"
AIRFLOW_VAR_DAG_HOME="/opt/airflow/dags"
AIRFLOW_VAR_DW_CONFIG_PATH="/opt/airflow/dags/dw_config"
AIRFLOW_VAR_DW_TEMPLATE_PATH="/opt/airflow/dags/templates"
AIRFLOW_VAR_DW_ETL_PATH="/opt/airflow/dags/dw_etl"
AIRFLOW_VAR_DW_SQL_PATH="/opt/airflow/dags/dw_sql"
AIRFLOW_VAR_DW_DEFAULT_SCHEMA="edw_core"
AIRFLOW_VAR_DW_LOG_RETENTION_DAYS=30

# ── Optional integrations ─────────────────────────────────────────────────────
AIRFLOW_VAR_SLACK_TOKEN=""
```

Save and close (`Ctrl+O`, `Enter`, `Ctrl+X`), then load into shell:

```bash
echo 'source ~/.airflow' >> ~/.bashrc
source ~/.bashrc
```

### Generate secret keys

```bash
# Fernet key (Airflow encryption)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret keys (Airflow webserver + Web UI)
python3 -c "import secrets; print(secrets.token_hex(32))"   # run twice — one for each key
```

Paste the outputs into `~/.airflow` for `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`, and `WEBUI_SECRET_KEY`.

---

## Step 6 — Fix file permissions

```bash
sudo chown -R $(id -u):0 ~/datapipeline/airflow
sudo chmod -R g+rwx ~/datapipeline/airflow
git config core.fileMode false
```

---

## Step 7 — Open firewall ports

```bash
sudo iptables -A INPUT -p tcp --dport 8090 -j ACCEPT   # Airflow webserver
sudo iptables -A INPUT -p tcp --dport 5001 -j ACCEPT   # Web UI
sudo iptables -A INPUT -p tcp --dport 8050 -j ACCEPT   # Pipeline Monitor
sudo iptables -A INPUT -p tcp --dport 9090 -j ACCEPT   # Jenkins
sudo iptables -A INPUT -p tcp --dport 5050 -j ACCEPT   # PgAdmin
sudo iptables -A INPUT -p tcp --dport 55432 -j ACCEPT  # PostgreSQL (external)
```

> On cloud providers (AWS, Azure, GCP) also open these ports in your Security Group / Firewall rules.

---

## Step 8 — Build and start the stack

```bash
cd ~/datapipeline/airflow
make
```

`make` automatically:
1. Sources `~/.airflow` to load all env vars
2. Runs `docker compose --env-file .env up -d --build`
3. Waits for `airflow-init` to complete (creates DB schema + admin user)
4. Prints all service URLs

First start takes **5–10 minutes** to pull images and build the custom Airflow image (JDBC drivers are downloaded during build).

Check that all containers are running:

```bash
docker compose ps
```

You should see these services with status `running` or `healthy`:

```
airflow-webserver
airflow-scheduler
airflow-worker
airflow-triggerer
airflow-init        (exits with code 0 after first run — normal)
postgres
redis
web-ui
pipeline-monitor
jenkins
pgadmin
```

---

## Step 9 — Verify Airflow is up

```bash
# Check the init log — should end with "exited with code 0"
docker compose logs airflow-init --tail=20
```

Open **http://\<server-ip\>:8090** and log in with `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `~/.airflow`.

---

## Step 10 — Set up Airflow Connections

Go to **Airflow UI → Admin → Connections → +** and add your source and target database connections.

| Field | Value |
|---|---|
| Connection Id | e.g. `cenfri_prd_con` |
| Connection Type | `Postgres` (or MySQL / MSSQL) |
| Host | your database hostname |
| Schema | your database name |
| Login | your database username |
| Password | your database password |
| Port | 5432 (Postgres) / 3306 (MySQL) / 1433 (MSSQL) |

Repeat for every source and target database your ETL pipelines need.

---

## Step 11 — Set up Jenkins CI/CD (auto-deploy on push)

### 11.1 Get the initial Jenkins admin password

```bash
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Open **http://\<server-ip\>:9090**, paste the password → **Install suggested plugins** → create your admin user.

### 11.2 Trust GitHub host keys inside Jenkins

```bash
docker exec -u root jenkins bash -c "
  mkdir -p /var/jenkins_home/.ssh &&
  ssh-keyscan -t rsa,ecdsa,ed25519 github.com >> /var/jenkins_home/.ssh/known_hosts &&
  chmod 600 /var/jenkins_home/.ssh/known_hosts &&
  chown -R jenkins:jenkins /var/jenkins_home/.ssh
"
```

### 11.3 Generate a dedicated deploy key

```bash
ssh-keygen -t ed25519 -C "jenkins-deploy@plengo" -f ~/.ssh/jenkins_deploy -N ""
cat ~/.ssh/jenkins_deploy.pub   # copy this — add to GitHub Deploy Keys
```

Add SSH config entry:

```bash
cat >> ~/.ssh/config << 'EOF'

Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/jenkins_deploy
    IdentitiesOnly yes
EOF
chmod 600 ~/.ssh/config
```

### 11.4 Add deploy key to GitHub

GitHub → **WASACIT/wasac_analytics → Settings → Deploy keys → Add deploy key**
- Title: `wasac-server-deploy`
- Key: paste output of `cat ~/.ssh/jenkins_deploy.pub`
- ✅ Allow write access → **Add key**

### 11.5 Add private key to Jenkins credentials

Jenkins → **Manage Jenkins → Credentials → System → Global → Add Credentials**
- Kind: `SSH Username with private key`
- ID: `github-deploy-key`
- Username: `git`
- Private key → **Enter directly** → paste output of `cat ~/.ssh/jenkins_deploy`
- Save

### 11.6 Create the deploy job

Jenkins → **New Item** → name: `wasac-deploy` → **Freestyle project** → OK

**Source Code Management → Git:**
- Repository URL: `git@github.com:WASACIT/wasac_analytics.git`
- Credentials: `github-deploy-key`
- Branch specifier: `*/main`

**Build Triggers:** ✅ **GitHub hook trigger for GITScm polling**

**Build Steps → Execute shell:**
```bash
chmod +x $WORKSPACE/deploy.sh
DEPLOY_TARGET=/deployment $WORKSPACE/deploy.sh
```

Save → **Build Now** to test.

### 11.7 Add GitHub webhook

GitHub → **WASACIT/wasac_analytics → Settings → Webhooks → Add webhook**
- Payload URL: `http://<server-ip>:9090/github-webhook/`
- Content type: `application/json`
- Trigger: **Just the push event**
- Save

> Every `git push` to `main` now auto-deploys to the server. ✅

---

## Daily operations

```bash
make              # start all services
make down         # stop all services
make restart      # stop + start
make logs         # tail all logs

# Single service
make restart-service s=pipeline-monitor
make logs-service s=airflow-scheduler
```

### Rebuild a single service after code changes

```bash
docker compose build pipeline-monitor
docker compose up -d pipeline-monitor
```

---

## Common issues

| Symptom | Fix |
|---|---|
| `docker: permission denied` | `sudo usermod -aG docker $USER && newgrp docker` |
| Airflow webserver not starting | Wait for `airflow-init` to exit 0 first: `docker compose logs airflow-init` |
| `source ~/.airflow: No such file or directory` | Create `~/.airflow` — see Step 5 |
| Blank values / connection refused in containers | Shell vars not loaded — run `source ~/.airflow` then `make down && make` |
| Pipeline Monitor shows no data | Check `DB_*` env vars inside container: `docker compose exec pipeline-monitor env \| grep DB_` |
| Port already in use | Change the host port in `docker-compose.yaml` (left side of `"8090:8080"`) |
| `Permission denied` on DAG files | Run Step 6 (fix permissions) then `make restart` |
| `Fernet key invalid` | Generate a new key and update `AIRFLOW__CORE__FERNET_KEY` in `~/.airflow`, then restart |

---

## Updating the stack

```bash
cd ~/datapipeline/airflow
git pull
make restart
```

If `Dockerfile` or `requirements.txt` changed, rebuild images:

```bash
make down
make
```

---

## Nuclear reset — wipe and reinstall

```bash
# Stop and remove all containers, images, and volumes for this stack only
docker ps -a | grep wasac | awk '{print $1}' | xargs -r docker stop
docker ps -a | grep wasac | awk '{print $1}' | xargs -r docker rm
docker images | grep wasac | awk '{print $3}' | xargs -r docker rmi
docker volume ls | grep wasac | awk '{print $2}' | xargs -r docker volume rm
docker network ls | grep wasac | awk '{print $1}' | xargs -r docker network rm

# Start fresh
make
```

---

## Support

📧 [datateam@cenfri.org](mailto:datateam@cenfri.org)  
🌐 [cenfri.org](https://cenfri.org)
