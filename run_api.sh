#!/bin/bash
# Script to run the API locally

echo "Starting FastAPI server..."
echo ""
echo "The API will be available at:"
echo "  - http://localhost:8000"
echo "  - Documentation: http://localhost:8000/docs"
echo "  - Health check: http://localhost:8000/health"
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run API
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
