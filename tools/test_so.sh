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

# sort rows
sort < ./input/oldpis-so.jsonl >./output/oldpis-so-sort.jsonl
sort < ./output/so.jsonl > ./output/so-sort.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./output/oldpis-so-sort.jsonl > ./output/oldpis-so-sort_keysort.jsonl
jq -cf ../../tools/diff.jq ./output/so-sort.jsonl > ./output/so-sort_keysort.jsonl

# sort the object keys
jq -s "." ./output/oldpis-so-sort_keysort.jsonl | jq --sort-keys "." | jq -c ".[]" > ./output/oldpis-so-sort_keysort_objsort.jsonl
jq -s "." ./output/so-sort_keysort.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/so-sort_keysort_objsort.jsonl

# we only need id and label so let's drop everything else from the old pis output
jq -c '{id, label}' ./output/oldpis-so-sort_keysort_objsort.jsonl > ./output/oldpis-so-sort_keysort_objsort_sel.jsonl

# compare the outputs
diff --color ./output/oldpis-so-sort_keysort_objsort_sel.jsonl ./output/so-sort_keysort_objsort.jsonl
