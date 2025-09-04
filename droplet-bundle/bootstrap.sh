#!/usr/bin/env bash
set -euo pipefail
sudo apt-get update -y
sudo apt-get upgrade -y
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
fi
sudo usermod -aG docker $USER
if ! docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE_VERSION="v2.27.0"
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -SL https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi
echo "Docker listo. Cierra y vuelve a entrar a la sesión SSH para usar 'docker' sin sudo."
