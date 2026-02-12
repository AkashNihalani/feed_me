#!/bin/sh
set -e

# Export current env for cron jobs
printenv | grep -v "no_proxy" > /etc/environment

cron -f
