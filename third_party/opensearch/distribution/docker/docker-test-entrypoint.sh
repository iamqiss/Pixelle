#!/usr/bin/env bash
set -e -o pipefail

cd /usr/share/density/bin/

/usr/local/bin/docker-entrypoint.sh | tee > /usr/share/density/logs/console.log
