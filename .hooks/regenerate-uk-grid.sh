#!/usr/bin/env bash
# Pre-commit hook: re-run uk_geo.py if it is staged, then stage the output image.
set -euo pipefail

if ! git diff --cached --name-only | grep -q '^uk-geo/uk_geo\.py$'; then
    exit 0
fi

echo "regenerate-uk-grid: uk_geo.py changed — regenerating grid plots..."

(cd uk-geo && poetry run python uk_geo.py)

git add uk-geo/plots/
echo "regenerate-uk-grid: staged uk-geo/plots/"
