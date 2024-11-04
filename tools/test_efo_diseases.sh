#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/efo ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/efo_diseases || exit
mkdir -p ./output

# get efo
curl -Ls https://github.com/EBISPOT/efo/releases/download/v3.70.0/efo_otar_slim.json > ./input/efo_otar_slim.json

# run the transformation
uv run ontoform efo_diseases ./input/efo_otar_slim.json ./output/efo_otar_slim_ontoform_diseases.jsonl

# disease file comparison
# sort diseases object keys and rows
jq -s "." ./input/diseases_efo-oldpis.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/oldpis_diseases_sort.jsonl
jq -s "." ./output/efo_otar_slim_ontoform_diseases.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/efo_otar_slim_ontoform_diseases_sort.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./output/oldpis_diseases_sort.jsonl > ./output/oldpis_diseases_sort_arrays.jsonl
jq -cf ../../tools/diff.jq ./output/efo_otar_slim_ontoform_diseases_sort.jsonl > ./output/efo_otar_slim_ontoform_diseases_sort_arrays.jsonl

# compare the outputs
diff --color ./output/oldpis_diseases_sort_arrays.jsonl ./output/efo_otar_slim_ontoform_diseases_sort_arrays.jsonl || :
