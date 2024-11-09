#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/disease ]; then
    echo "Please run this script from the root of the repository (like: ./tools/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/disease || exit
mkdir -p ./output

# get efo
curl -Ls https://github.com/EBISPOT/efo/releases/download/v3.70.0/efo_otar_slim.json > ./input/efo_otar_slim.json

# run the transformation
uv run ontoform disease ./input/efo_otar_slim.json ./output/ontoform-disease.jsonl --format ndjson

# sort rows
sort < ./input/oldetl-disease.jsonl    > ./output/oldetl-disease-sort.jsonl
sort < ./output/ontoform-disease.jsonl > ./output/ontoform-disease-sort.jsonl

# drop description, we already know they will be different
jq -c 'del(.description)' ./output/oldetl-disease-sort.jsonl    > ./output/oldetl-disease-sort_nodesc.jsonl
jq -c 'del(.description)' ./output/ontoform-disease-sort.jsonl > ./output/ontoform-disease-sort_nodesc.jsonl

# drop not-implemented parts:
# indirectLocationIds
jq -c 'del(.indirectLocationIds)' ./output/ontoform-disease-sort_nodesc.jsonl > ./output/ontoform-disease-sort_nodesc_dropped.jsonl
jq -c 'del(.indirectLocationIds)' ./output/oldetl-disease-sort_nodesc.jsonl > ./output/oldetl-disease-sort_nodesc_dropped.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./output/oldetl-disease-sort_nodesc_dropped.jsonl   > ./output/oldetl-disease-sort_nodesc_dropped_arraysort.jsonl
jq -cf ../../tools/diff.jq ./output/ontoform-disease-sort_nodesc_dropped.jsonl > ./output/ontoform-disease-sort_nodesc_dropped_arraysort.jsonl

# sort the object keys
jq -s "." ./output/oldetl-disease-sort_nodesc_dropped_arraysort.jsonl | jq --sort-keys "." | jq -c ".[]"   > ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort.jsonl
jq -s "." ./output/ontoform-disease-sort_nodesc_dropped_arraysort.jsonl | jq --sort-keys "." | jq -c ".[]" > ././output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort.jsonl

# remove nulls
jq -c "del(..|nulls)" ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort.jsonl     > ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl
jq -c "del(..|nulls)" ././output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort.jsonl > ./output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl

# remove empty arrays
jq -c "del(.. | select(. == []))" ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl   > ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl
jq -c "del(.. | select(. == []))" ./output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl > ./output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl

# compare the outputs
diff --color ./output/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl ./output/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl || :
