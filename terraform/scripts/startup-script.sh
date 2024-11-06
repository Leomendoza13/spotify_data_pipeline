#!/bin/bash

# Copyright (c) 2024 Léo Mendoza
# Licensed under the MIT License. See LICENSE file in the project root for detail

# Update and install certificates
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl

# Create the directory for Docker's key and add the key
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the Docker repository to sources.list
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update and install Docker
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Create directories for Airflow
sudo mkdir -p /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config
echo -e "AIRFLOW_UID=$(id -u)" > /opt/airflow/.env


# Download the docker-compose.yaml file to /opt/airflow
dockerComposeUrl="https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml"
outputPath="/opt/airflow/docker-compose.yaml"
echo "Téléchargement du fichier docker-compose.yaml depuis ${dockerComposeUrl}"
downloadFile=$(sudo curl -Lf "$dockerComposeUrl" -o "$outputPath")

# Add extra host for VM metadata access in required containers
sudo sed -i '/airflow-webserver:/a\ \ \ \ extra_hosts:\n\ \ \ \ \ \ - "metadata.google.internal:169.254.169.254"' "$outputPath"
sudo sed -i '/airflow-scheduler:/a\ \ \ \ extra_hosts:\n\ \ \ \ \ \ - "metadata.google.internal:169.254.169.254"' "$outputPath"
sudo sed -i '/airflow-worker:/a\ \ \ \ extra_hosts:\n\ \ \ \ \ \ - "metadata.google.internal:169.254.169.254"' "$outputPath"

# Execute Docker Compose for Airflow
cd /opt/airflow
sudo apt install -y docker-compose
sudo docker compose -f docker-compose.yaml -p airflow_project up airflow-init
sudo docker compose -f docker-compose.yaml -p airflow_project up -d


