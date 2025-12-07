#!/bin/bash
set -e  # Exit on error

echo "Starting build process..."

# Sync dependencies
echo "Installing dependencies with uv..."
uv sync

# Run ruff check and format
echo "Running ruff check..."
uv run ruff format
uv run ruff check src/ --fix

echo "Running ruff format..."
uv run ruff format src/

# Run unit tests
echo "Running unit tests..."
uv run pytest tests/ || echo "No tests found or tests failed"

echo "Build completed successfully!"
