#!/bin/bash
# Script to run the project locally

set -e

echo "Event Stream Data Pipeline - Local Setup"
echo "============================================="

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo -e "\n Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "\n Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo -e "\n Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Run data pipeline
echo -e "\n Running data pipeline..."
python src/pipeline.py

# Show instructions to run the API
echo -e "\n Setup completed!"
echo ""
echo "To start the API, run:"
echo ""
echo -e " uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000"
echo ""
echo "Or simply:"
echo ""
echo -e " ./run_api.sh"
echo ""
