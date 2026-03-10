#!/bin/bash
set -e

echo "Starting Event Stream Data Pipeline..."

echo "Cleaning data and generating aggregations..."
python src/pipeline.py

echo "Starting FastAPI server..."
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
