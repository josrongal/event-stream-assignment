#!/bin/bash
# Script to run tests

echo "Running tests..."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run pytest
pytest tests/ -v --cov=src --cov-report=term-missing

echo ""
echo "Tests completed"
