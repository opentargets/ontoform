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
mkdir -p $work_dir/input/disease

# get efo
# DiseaseTransformer —
# input/disease/efo_otar_slim.json -> output/disease/disease.parquet
curl -Ls https://github.com/EBISPOT/efo/releases/download/v3.70.0/efo_otar_slim.json > $work_dir/input/disease/efo_otar_slim.json
gsutil cp gs://open-targets-pre-data-releases/24.09dev/output/oldetl_diseases.jsonl $work_dir/oldetl-disease.jsonl

# run ontoform
uv run ontoform --work-dir $work_dir disease --output-format ndjson

# sort rows
sort < $work_dir/oldetl-disease.jsonl         > /tmp/oldetl-disease-sort.jsonl
sort < $work_dir/output/disease/disease.jsonl > /tmp/ontoform-disease-sort.jsonl

# drop description, we already know they will be different
jq -c 'del(.description)' /tmp/oldetl-disease-sort.jsonl   > /tmp/oldetl-disease-sort_nodesc.jsonl
jq -c 'del(.description)' /tmp/ontoform-disease-sort.jsonl > /tmp/ontoform-disease-sort_nodesc.jsonl

# drop not-implemented parts:
# indirectLocationIds
jq -c 'del(.indirectLocationIds)' /tmp/oldetl-disease-sort_nodesc.jsonl   > /tmp/oldetl-disease-sort_nodesc_dropped.jsonl
jq -c 'del(.indirectLocationIds)' /tmp/ontoform-disease-sort_nodesc.jsonl > /tmp/ontoform-disease-sort_nodesc_dropped.jsonl

# sort the arrays
jq -cf ./tools/diff.jq /tmp/oldetl-disease-sort_nodesc_dropped.jsonl   > /tmp/oldetl-disease-sort_nodesc_dropped_arraysort.jsonl
jq -cf ./tools/diff.jq /tmp/ontoform-disease-sort_nodesc_dropped.jsonl > /tmp/ontoform-disease-sort_nodesc_dropped_arraysort.jsonl

# sort the object keys
jq -s "." /tmp/oldetl-disease-sort_nodesc_dropped_arraysort.jsonl   | jq --sort-keys "." | jq -c ".[]" > /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort.jsonl
jq -s "." /tmp/ontoform-disease-sort_nodesc_dropped_arraysort.jsonl | jq --sort-keys "." | jq -c ".[]" > /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort.jsonl

# remove nulls
jq -c "del(..|nulls)" /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort.jsonl   > /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl
jq -c "del(..|nulls)" /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort.jsonl > /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl

# remove empty arrays
jq -c "del(.. | select(. == []))" /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl   > /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl
jq -c "del(.. | select(. == []))" /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls.jsonl > /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl

# compare the outputs
diff --color /tmp/oldetl-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl /tmp/ontoform-disease-sort_nodesc_dropped_arraysort_objsort_nonulls_noempty.jsonl || :
