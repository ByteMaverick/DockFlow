# Airflow Setup on VM

## Updated Installation Guide – Apache Airflow on GCP Compute Engine (2025)

### Step 1: Create a Compute Engine Instance

1. In Google Cloud Console, search for "VM instances" → Create Instance
2. Recommended specs for small to medium Airflow setups:
   - **Machine type**: e2-standard-2 (2 vCPU, 8 GB RAM)
   - **Boot disk**: Debian 12 (Bookworm) or Ubuntu 22.04 LTS (Debian 10/11 also works, but Debian 12 is more future-proof)
   - **Disk size**: 50 GB SSD
3. **Firewall**: Check "Allow HTTP" and "Allow HTTPS"
4. Click **Create**

### Step 2: Connect via SSH & Install Basics

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv libpq-dev curl
```

> **Note**: If a new window pops up, just press enter to keep the current version installed

### Step 3: Create a Python Virtual Environment

Instead of old Conda/Miniconda steps, use Python's built-in venv:

```bash
mkdir ~/airflow_demo && cd ~/airflow_demo
python3 -m venv venv
source venv/bin/activate
```

### Step 4: Install Airflow (Latest Version)

Airflow now recommends using constraints to avoid dependency conflicts:

```bash
export AIRFLOW_VERSION=3.0.4
export PYTHON_VERSION="$(python --version | awk '{print $2}' | cut -d. -f1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install --upgrade pip
pip install "apache-airflow[gcp,postgres,celery]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

**Verification Steps:**
- Check version: `airflow version`
- Check commands: `airflow --help`
- Run using: `airflow standalone`

Get the username and password from the command line output: `{username: password}`

### Step 5: Running Airflow in the Background

In the current setup, the airflow server will shutdown the moment you close the terminal.

To make the airflow server run in the background, use:

#### Run in the background with nohup

```bash
nohup airflow standalone > airflow.log 2>&1 &
```

**Benefits:**
- Runs Airflow in the background
- Output goes to `airflow.log`
- You can close your SSH session, and it will still run

#### To stop the background process later:

```bash
pkill -f "airflow standalone"
```

---

## Additional Notes

- The setup uses Python's built-in virtual environment instead of Conda for better compatibility
- Airflow 3.0.4 is the latest stable version as of 2025
- The constraint file ensures dependency compatibility
- Background execution allows persistent operation even after SSH disconnection