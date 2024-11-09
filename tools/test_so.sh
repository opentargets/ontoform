#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/so ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/so || exit
mkdir -p ./output

# get so
curl -Ls https://raw.githubusercontent.com/The-Sequence-Ontology/SO-Ontologies/refs/heads/master/Ontology_Files/so.json > ./input/so.json

# run the transformation
uv run ontoform so ./input/so.json ./output/so.jsonl --format ndjson

# drop definitions, we already know they will be different
# and the definition alternatives, which are not there in the ontoform output
# also, sort rows
jq -c '{id:.id,label:.label}' ./input/so-oldpis.jsonl | sort > ./output/so-oldpis_sort.jsonl

# sort the object keys
jq -s "." ./output/so-oldpis_sort.jsonl | jq --sort-keys "." | jq -c ".[]" > ./output/so-oldpis_sort_keys.jsonl
jq -s "." ./output/so.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/so_keys.jsonl

# compare the outputs
diff --color ./output/so-oldpis_sort_keys.jsonl ./output/so_keys.jsonl
