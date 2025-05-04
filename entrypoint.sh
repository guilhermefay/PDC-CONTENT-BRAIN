#!/usr/bin/env sh
set -eu

echo "--- ENTRYPOINT: Trying simple python print --- "

python3 -c "import sys; print('--- PYTHON: Hello from simple Python! ---', file=sys.stderr)"

echo "--- ENTRYPOINT: Simple python print finished --- "

# exit 0 # Comentado para ver se o container continua ou falha depois 