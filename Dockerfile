# Dockerfile for Event Stream Data Pipeline
FROM python:3.12-slim

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry

# Install poetry-plugin-export (required for export command)
RUN pip install --no-cache-dir poetry-plugin-export

# Copy dependency files
COPY pyproject.toml poetry.lock .

# Generate requirements.txt from poetry.lock
RUN poetry export -f requirements.txt --output requirements.txt

# Install dependencies from generated requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY data/bronze ./data/bronze

# Expose API port
EXPOSE 8000

# Entrypoint script to run pipeline + API
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
