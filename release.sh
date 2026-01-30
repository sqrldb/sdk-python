#!/bin/bash
set -e

cd "$(dirname "$0")"

VERSION="0.1.0"

echo "Releasing squirreldb-sdk v${VERSION}..."

# Install build dependencies
echo "Installing build dependencies..."
pip install --quiet build twine pytest

echo "Running tests..."
python -m pytest tests/ -q

echo "Building..."
rm -rf dist/ build/ *.egg-info
python -m build

echo "Publishing to PyPI..."
python -m twine upload dist/*

echo "Released squirreldb-sdk@${VERSION}"
echo ""
echo "Users can install with:"
echo "  pip install squirreldb-sdk"
