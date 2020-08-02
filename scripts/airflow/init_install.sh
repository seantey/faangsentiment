#!/bin/bash
cd ~

# Prepare for Docker installation.
# https://docs.docker.com/engine/install/ubuntu/
sudo apt update -y
sudo apt upgrade -y

# Update the apt package index and install packages to allow apt to use a repository over HTTPS
sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

# Add Docker’s official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo apt-key fingerprint 0EBFCD88

# set up the stable repository.

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

# Install Docker

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# Install Docker Compose, might want to change version 1.26.2 to newer versions in the future
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Run Airflow using the docker-airflow image to avoid seting up the varios components needed to run airflow.
# https://github.com/puckel/docker-airflow

sudo docker pull puckel/docker-airflow

# Docker-airflow uses the sequential executor by default which does not parallelize the tasks
# Instead we will use the local executor which does have parallel task execution but all on the same machine.
# We may change to using the Celery executor in the future to run on multiple machines.

git clone https://github.com/puckel/docker-airflow.git

# Fetch a copy of my repo to access the dag files.
git clone https://github.com/seantey/faangsentiment.git

# Copy dags to docker-airflow dag folder which is mapped into the container due to settings in compose yml.
cp -r faangsentiment/scripts/airflow/dags/* docker-airflow/dags/

# Add requirements txt to docker-airflow folder
cp faangsentiment/scripts/airflow/requirements.txt docker-airflow/requirements.txt

# Add a statement to the docker compose file to ensure that the requirements file is mounted
sed -i '/volumes:/a \            - ./requirements.txt:/requirements.txt' docker-airflow/docker-compose-LocalExecutor.yml

# This will start the airflow server and the postgres server (airflow metadata)
sudo docker-compose -f docker-airflow/docker-compose-LocalExecutor.yml up -d

# On your local machine, you might want to set up a SSH tunnel to map the Airflow Server UI to localhost.
# https://stackoverflow.com/questions/46090750/how-to-setup-ssh-tunneling-in-aws

# Run this locally to map the localhost:8080 on the remote machine (which maps to a server in the container)
# to your port 8080

# ssh -i /path/to/key -N -L 8080:localhost:8080 user@yourserver.com

# The terminal should be blank after the command but going to 0.0.0.0:8080 or localhost:8080 should show the UI.
