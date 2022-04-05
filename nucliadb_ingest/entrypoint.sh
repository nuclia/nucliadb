#!/bin/bash
set -e

# remove self from swim peers list
export SWIM_PEERS_ADDR=$(echo $SWIM_PEERS_ADDR | sed -re "s|\"$HOSTNAME\.([a-zA-Z0-9]+\.?)+(:[0-9]+)?\",?||g" | sed -re "s|,\s*]|]|g")

# parse partition number from hostname
export REPLICA_NUMBER=$(echo $HOSTNAME | grep -oE '[0-9]+$')

# Get number of replicas from StatefulSet configuration
## Point to the internal API server hostname
APISERVER=https://kubernetes.default.svc
## Path to ServiceAccount token
SERVICEACCOUNT=/var/run/secrets/kubernetes.io/serviceaccount
## Read this Pod's namespace
NAMESPACE=$(cat ${SERVICEACCOUNT}/namespace)
## Read the ServiceAccount bearer token
TOKEN=$(cat ${SERVICEACCOUNT}/token)
## Reference the internal certificate authority (CA)
CACERT=${SERVICEACCOUNT}/ca.crt
## Explore the API with TOKEN
export TOTAL_REPLICAS=$(curl --silent --cacert ${CACERT} --header "Authorization: Bearer ${TOKEN}" -X GET ${APISERVER}/apis/apps/v1/namespaces/${NAMESPACE}/statefulsets/ingest | jq '.status.replicas')

exec "$@"
