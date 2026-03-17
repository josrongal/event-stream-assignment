#!/bin/bash
# Script to setup the project locally using Poetry

set -e

echo "Event Stream Data Pipeline - Local Setup"
echo "============================================="

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    echo "Error: Poetry is not installed."
    echo "Install Poetry from: https://python-poetry.org/docs/#installation"
    exit 1
fi

# Install dependencies using Poetry
echo -e "\n Installing dependencies with Poetry..."
poetry install

# Run data pipeline
echo -e "\n Running data pipeline..."
poetry run python src/pipeline.py

# Show instructions to run the API
echo -e "\n Setup completed!"
echo ""
echo "To start the API, run:"
echo ""
echo -e " poetry run uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000"
echo ""
echo "Or simply:"
echo ""
echo -e " ./run_api.sh"
echo ""
