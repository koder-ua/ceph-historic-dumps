#!/usr/bin/env bash
set -o pipefail
set -o errexit
set -o nounset
set -x

readonly NODES="$1"

bash deploy.sh --clean "${NODES}"
bash deploy.sh --show "${NODES}"
bash deploy.sh --deploy "${NODES}"
bash deploy.sh --start --parallel "${NODES}"
bash deploy.sh --show --parallel "${NODES}"
bash deploy.sh --clean --parallel --jobs 2 "${NODES}"
bash deploy.sh --deploy --parallel "${NODES}"
bash deploy.sh --start "${NODES}"
sleep 2
bash deploy.sh --collect "${NODES}"
bash deploy.sh --collect --parallel --jobs 3 "${NODES}"
bash deploy.sh --redeploy "${NODES}"
bash deploy.sh --collect --parallel "${NODES}"
bash deploy.sh --clean "${NODES}"
bash deploy.sh --show "${NODES}"
echo "Passed successfully!"
