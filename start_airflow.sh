#!/bin/bash

# Gen SSH key used for fstp server
ssh-keygen -t ed25519 -f ssh_host_ed25519_key < /dev/null
ssh-keygen -t rsa -b 4096 -f ssh_host_rsa_key < /dev/null

# Docker compose up
docker-compose up -d

trap "docker compose stop" EXIT
