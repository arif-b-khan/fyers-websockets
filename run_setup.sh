#!/bin/zsh
# Creates a virtualenv in .venv and installs project requirements
set -euo pipefail

PYTHON=${PYTHON:-python3}
VENV_DIR=".venv"

if [ -d "$VENV_DIR" ]; then
  echo "Virtual environment already exists at $VENV_DIR"
else
  echo "Creating virtual environment at $VENV_DIR using $PYTHON"
  $PYTHON -m venv "$VENV_DIR"
fi

# Activate and upgrade pip
source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip

# Install requirements
if [ -f requirements.txt ]; then
  pip install -r requirements.txt
else
  echo "requirements.txt not found"
  exit 1
fi

echo "Running smoke test: python -c 'import flask, websockets, sqlalchemy, protobuf, requests'"
python - <<'PY'
try:
    import flask, websockets, sqlalchemy, google, requests
    print('Smoke test passed: core packages importable')
except Exception as e:
    print('Smoke test failed:', e)
    raise
PY

echo "Setup complete. Activate the environment with: source $VENV_DIR/bin/activate"