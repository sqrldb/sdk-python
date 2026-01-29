#!/bin/bash
set -e

cd "$(dirname "$0")"

VERSION=$(grep 'version' pyproject.toml | head -1 | awk -F'"' '{print $2}')

echo "Building squirreldb Python SDK v${VERSION}..."

rm -rf dist/ build/ *.egg-info

python -m build

echo "Running tests..."
python -m pytest tests/

echo "Publishing to PyPI..."
python -m twine upload dist/*

echo "Published squirreldb==${VERSION} to PyPI"
