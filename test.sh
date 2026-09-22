#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
./bin/unit_tests
"${PYTHON:-python3}" tests/integration_test.py
