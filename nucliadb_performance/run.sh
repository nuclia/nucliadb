#!/bin/bash
KB=${1:-small}

echo "Installing requirements..."
python3 -m venv performance_env
source performance_env/bin/activate
python3 -m pip install -q --upgrade pip wheel
pip install -q -e .
pip install -q -e ../nucliadb 

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
python export_import.py --action=import --kb=$KB --uri=https://storage.googleapis.com/nucliadb_indexer/nucliadb_performance/exports/$KB.export

echo "Running performance tests..."
make test-standalone-search kb_slug=$KB max_workers=4 ramp_up=2 duration_s=60

# Show results
cat standalone.json

echo "Stopping NucliaDB at ${NDB_PID}..."
kill $NDB_PID

echo "Cleaning up..."
source deactivate
# rm -rf ./performance_env
rm ./cache.data.db
rm -rf ./data
# rm -rf ./logs