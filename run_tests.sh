#!/bin/bash
# Script to run tests with Poetry

echo "Running tests..."
echo ""

# Run pytest with Poetry
poetry run pytest tests/ -v --cov=src --cov-report=term-missing

echo ""
echo "Tests completed"
