#!/usr/bin/env bash
set -x
set -e

bash deploy.sh -c
bash deploy.sh -s
bash deploy.sh -d
bash deploy.sh -s
bash deploy.sh -l
bash deploy.sh -L
bash deploy.sh -r
bash deploy.sh -L
bash deploy.sh -c
bash deploy.sh -s
