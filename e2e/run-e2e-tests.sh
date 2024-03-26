# /bin/sh

set -e

/opt/nucliadb/bin/pip install -r e2e/requirements.txt
source /opt/nucliadb/bin/activate
export standalone_node_port=10009
export DATA_PATH=data1
export standalone_node_role=index
nucliadb --http-port=8080 &
export standalone_node_port=10010
export DATA_PATH=data2
export standalone_node_role=index
nucliadb --http-port=8081 &
export standalone_node_port=99999
export DATA_PATH=foobar
export standalone_node_role=worker
nucliadb --http-port=8082 &
pytest -s -vv --tb=native e2e/test_e2e.py