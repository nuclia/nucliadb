#!/bin/bash
RUN_ID=${1:-`date +%Y%m%d%H%M%S`}
KB=${2:-small}
RELEASE_CHANNEL=${3:-stable}

KB_SLUG=$KB-$RELEASE_CHANNEL
GCS_BUCKET="https://storage.googleapis.com/nucliadb_indexer/nucliadb_performance/exports"
EXPORT_FILE="$KB.export"
BENCHMARK_OUTPUT_FILE="standalone_$RUN_ID.json"

VENV=venv_$RUN_ID

echo "Installing requirements at $VENV..."
python3 -m venv $VENV
source $VENV/bin/activate
python3 -m pip install -q --upgrade pip wheel
pip install -q -e .
pip install -q -e ../nucliadb

echo "Waiting for lock..."
python lock.py --action=acquire

echo "Starting NucliaDB..."
DEBUG=true nucliadb &
NDB_PID=$!
echo "NucliaDB PID: $NDB_PID"

echo "Waiting for NucliaDB to start..."
while : ; do
    sleep 1
    status_code=`curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/api/v1/health/ready`
    if [ "$status_code" -eq "200" ]; then
        sleep 2
        echo "NucliaDB is ready!"
        break
    fi
done

echo "Importing data..."
python export_import.py --action=import --kb=$KB_SLUG --uri=$GCS_BUCKET/$EXPORT_FILE --release_channel=$RELEASE_CHANNEL

echo "Running performance tests..."
make test-standalone-search benchmark_output=$BENCHMARK_OUTPUT_FILE kb_slug=$KB_SLUG max_workers=4 ramp_up=2 duration_s=60

# Show results
cat $BENCHMARK_OUTPUT_FILE

echo "Stopping NucliaDB at ${NDB_PID}..."
kill $NDB_PID

echo "Releasing lock..."
python lock.py --action=release

echo "Cleaning up..."
source deactivate
# rm -rf $VENV
rm ./cache.data.db
# rm -rf ./data
# rm -rf ./logs