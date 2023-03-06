#!/usr/bin/env bash

# define a log folder

logs=$(pwd)/logs

# define an iso8601 timetstamp

iso8601=$(date +"%Y%m%dT%H%M%S")

clear

echo '# Ensure connection string is available as ENV_VAR'

source ~/.microsoft/usersecrets/pj/adl/demo/env_vars.sh

echo '# Ensure log folder exists'
mkdir -p "$logs"
echo

echo '# Execute the script and log with tee'
#./avroidl2parquet.fsx 10000 | tee -a "$logs/$iso8601.log"
./avroidl2parquet.fsx  2048 | tee -a "$logs/$iso8601.log"
#./avroidl2parquet.fsx   256 | tee -a "$logs/$iso8601.log"
#./avroidl2parquet.fsx     1 | tee -a "$logs/$iso8601.log"
echo
