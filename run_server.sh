#!/bin/bash

memory=$1
echo -J-Xmx"$memory" -J-Xms"$memory"
echo "./run_server.sh memSize cycle numWorkers numPerWorker"
sbt -J-Xmx"$memory" -J-Xms"$memory" "run $2 $3 $4"
