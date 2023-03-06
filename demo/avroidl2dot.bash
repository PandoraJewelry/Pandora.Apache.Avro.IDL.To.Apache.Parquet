#!/usr/bin/env bash

# define a log folder

logs=$(pwd)/logs
dots=$(pwd)/dots

# define an iso8601 timetstamp

iso8601=$(date +"%Y%m%dT%H%M%S")

clear

echo '# Ensure log folder exists'
rm    -frv "$logs"
mkdir -p   "$logs"
echo

echo '# Ensure dot folder exists'
rm    -frv "$dots"
mkdir -p   "$dots"
echo

echo '# Execute the script and log'
./avroidl2dot.fsx | tee -a "$logs/$iso8601.log"
echo

echo '# Generate SVG and PNG files from DOT files'
for f in $(find $dots -name "*.dot")
do
    echo "Generating SVG from $f"
    dot -T svg $f > $f.svg
    echo "Generating PNG from $f"
    dot -T png $f > $f.png
done
echo
