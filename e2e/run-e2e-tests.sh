# /bin/sh

set -e

/opt/nucliadb/bin/pip install -r e2e/requirements.txt
while ! pg_isready -h localhost -p 5432 -U postgres -d postgres; do
    sleep 1
done
source /opt/nucliadb/bin/activate
export standalone_node_port=10009
export DATA_PATH=data1
nucliadb --http-port=8080 &
export standalone_node_port=10010
export DATA_PATH=data2
nucliadb --http-port=8081 &
pytest -s --tb=native e2e/test_e2e.py