#!/usr/bin/env bash
set -o pipefail
set -o errexit
set -o nounset

bash deploy.sh --clean
bash deploy.sh --show
bash deploy.sh --deploy
bash deploy.sh --start --parallel
bash deploy.sh --show --parallel
bash deploy.sh --clean --parallel --jobs 2
bash deploy.sh --deploy --parallel
bash deploy.sh --start
sleep 2
bash deploy.sh --collect
bash deploy.sh --collect --parallel --jobs 3
bash deploy.sh --redeploy
bash deploy.sh --collect --parallel
bash deploy.sh --clean
bash deploy.sh --show
