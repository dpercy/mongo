#!/bin/bash
set -xeu

i=0
while ! grep -q 'error loading.*system.js' out
do
    i=$((i + 1))
    ./venv/bin/python3 buildscripts/resmoke.py --basePort=40000 --dbpathPrefix ~/tmp --suite=parallel jstests/parallel/basicPlus.js > "out.$i" || true
    cp "out.$i" out
done
echo "Found an example after $i iterations"
