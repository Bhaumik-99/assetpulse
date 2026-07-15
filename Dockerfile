FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY config/ config/
COPY scripts/ scripts/
COPY dashboard/ dashboard/
COPY airflow/ airflow/
COPY dbt/ dbt/
COPY pyproject.toml .

RUN pip install -e .

ENV ASSETPULSE_ENV=production
ENV ASSETPULSE_CONFIG_PATH=config/development.yaml

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.headless=true"]
