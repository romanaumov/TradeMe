#!/bin/bash
set -euo pipefail

HOME="/home/trademe"
LOG_FILE="$HOME/startup_script.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date)] Startup script started."

# System updates
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip git docker.io curl wget

# Docker configuration
sudo usermod -aG docker trademe
sudo systemctl enable docker
sudo systemctl start docker

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/${docker_compose_version}/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Clone the repository if not already cloned
REPO_DIR="/home/trademe/trademe-viewer"
# GIT_REPO="https://${github_username}:${github_token}@github.com/romanaumov/AI-Video-Hierarchy.git"
GIT_REPO="https://github.com/romanaumov/TradeMe.git"

if [ ! -d "$REPO_DIR" ]; then
    echo "[$(date)] Cloning repository using a personal access token..."
    sudo -u trademe git clone "$GIT_REPO" "$REPO_DIR"
else
    echo "[$(date)] Repository already exists, skipping clone."
fi

# Set proper permissions
echo "[$(date)] Setting permissions..."
chown -R trademe:trademe "$REPO_DIR"
chmod -R 755 "$REPO_DIR"

# Set up the environment variables
echo "[$(date)] Set up the environment variables..."
pip install --upgrade pip
pip install google-cloud-secret-manager
cd "$REPO_DIR"
# Load the GCP secrets
python3 load_gcp_secrets.py "trademe-viewer" "ENV_VAR_TRADEME" "$REPO_DIR/.env"
python3 load_gcp_secrets.py "trademe-viewer" "CRED_VAR_TRADEME" "$REPO_DIR/airflow/keys/credentials.json"
python3 load_gcp_secrets.py "trademe-viewer" "SERV_VAR_TRADEME" "$REPO_DIR/airflow/keys/trademe-viewer-service-account.json"

# Build and run the video-hierarchy recommendation containers
docker-compose build --no-cache
docker-compose up -d

echo "[$(date)] Startup script completed successfully."
# Mark script as finished
touch "$HOME/startup_script_completed"
# Check if the log file is open and being written to
while lsof $HOME/startup_script.log >/dev/null; do
    sleep 1  # Wait for 1 second before checking again
done
# Once the file is no longer open, change ownership
chown -R trademe:trademe "$HOME"