@echo off
chcp 65001 >nul
cd /d "%~dp0"

docker info >nul 2>&1
if errorlevel 1 (
    echo Docker nao esta em execucao. Inicie o Docker Desktop e rode o script novamente.
    pause
    exit /b 1
)

if not exist logs mkdir logs
if not exist data mkdir data
if not exist .env echo AIRFLOW_UID=50000> .env

echo Iniciando o programa Apache Airflow
docker compose up airflow-init
if errorlevel 1 (
    echo Falha na inicializacao do Airflow.
    pause
    exit /b 1
)

cls

echo Subindo os containers
docker compose up -d

echo Operação finalizada. Acesse http://localhost:8080
pause
