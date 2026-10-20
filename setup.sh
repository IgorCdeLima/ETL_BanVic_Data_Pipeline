#!/bin/bash
set -e
cd "$(dirname "$0")"

if ! docker info > /dev/null 2>&1; then
    echo "Docker não está em execução. Inicie o Docker e rode o script novamente."
    exit 1
fi

mkdir -p logs data

if [ ! -f .env ]; then
    echo "AIRFLOW_UID=$(id -u)" > .env
fi

echo "🔹 Iniciando o programa Apache Airflow..."
docker compose up airflow-init
clear
echo "🔹 Subindo os containers..."
docker compose up -d
echo "✅ Operação finalizada. Acesse http://localhost:8080"
read -p "Pressione Enter para fechar o script..."
