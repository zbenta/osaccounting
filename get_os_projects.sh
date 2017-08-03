#!/bin/bash

# Get Openstack projects and saves into json format
# Require openstack admin credentials
OUT_DIR=${OUT_DIR:-"/tmp"}
openstack project list --domain default -f json --long > ${OUT_DIR}/projects.json

