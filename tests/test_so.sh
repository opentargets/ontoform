#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./tests ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

work_dir=/tmp

# set up stuff
set -x
set -e
mkdir -p $work_dir/input/so

# get so
# SOTransformer —
# input/so/so.json -> input/so/so.parquet
curl -Ls https://raw.githubusercontent.com/The-Sequence-Ontology/SO-Ontologies/refs/heads/master/Ontology_Files/so.json > $work_dir/input/so/so.json
gsutil cp gs://open-targets-pre-data-releases/24.09dev/input/so-inputs/so.json $work_dir/oldpis-so.jsonl

# run the transformation
uv run ontoform --work-dir $work_dir so --output-format ndjson

# sort rows
sort < $work_dir/oldpis-so.jsonl          > /tmp/oldpis-so-sort.jsonl
sort < $work_dir/intermediate/so/so.jsonl > /tmp/ontoform-so-sort.jsonl

# sort the arrays
jq -cf ./tools/diff.jq /tmp/oldpis-so-sort.jsonl   > /tmp/oldpis-so-sort_keysort.jsonl
jq -cf ./tools/diff.jq /tmp/ontoform-so-sort.jsonl > /tmp/ontoform-so-sort_keysort.jsonl

# sort the object keys
jq -s "." /tmp/oldpis-so-sort_keysort.jsonl   | jq --sort-keys "." | jq -c ".[]" > /tmp/oldpis-so-sort_keysort_objsort.jsonl
jq -s "." /tmp/ontoform-so-sort_keysort.jsonl | jq --sort-keys "." | jq -c ".[]" > /tmp/ontoform-so-sort_keysort_objsort.jsonl

# we only need id and label so let's drop everything else from the old pis output
jq -c '{id, label}' /tmp/oldpis-so-sort_keysort_objsort.jsonl > /tmp/oldpis-so-sort_keysort_objsort_sel.jsonl

# compare the outputs
diff --color /tmp/oldpis-so-sort_keysort_objsort_sel.jsonl /tmp/ontoform-so-sort_keysort_objsort.jsonl
