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
mkdir -p $work_dir/input/expression

# get expression files
# NormalTissueTransformer —
# input/expression/normal_tissue.tsv.zip -> input/expression-transformed/normal_tissue.tsv.gz
curl -Ls https://www.proteinatlas.org/download/tsv/normal_tissue.tsv.zip > $work_dir/input/expression/normal_tissue.tsv.zip

# TissueTransformer —
# input/expression/map_with_efos.json -> input/expression-transformed/tissue-translation-map.parquet
curl -Ls https://raw.githubusercontent.com/opentargets/expression_hierarchy/master/process/map_with_efos.json > $work_dir/input/expression/map_with_efos.json
gsutil cp gs://open-targets-pre-data-releases/24.09dev/input/expression-inputs/tissue-translation-map.json $work_dir/oldpis-tissue-translation-map.json

# run ontoform
uv run ontoform --work-dir /tmp expression --output-format ndjson

# we cannot compare with the old pis output because we do not know about version
# control in protein atlas so we cant get the same original file
diff <(unzip -p $work_dir/input/expression/normal_tissue.tsv.zip normal_tissue.tsv) <(gzip -d $work_dir/intermediate/expression/normal_tissue.tsv.gz -c)

# sort rows
sort < $work_dir/oldpis-tissue-translation-map.json                   > /tmp/oldpis-tissue-translation-map-sort.jsonl
sort < $work_dir/intermediate/expression/tissue-translation-map.jsonl > /tmp/ontoform-tissue-translation-map-sort.jsonl

# remove whitespace
jq -c . /tmp/oldpis-tissue-translation-map-sort.jsonl   > /tmp/oldpis-tissue-translation-map-sort_nowhite.jsonl
jq -c . /tmp/ontoform-tissue-translation-map-sort.jsonl > /tmp/ontoform-tissue-translation-map-sort_nowhite.jsonl

# compare the outputs
diff --color /tmp/oldpis-tissue-translation-map-sort_nowhite.jsonl /tmp/ontoform-tissue-translation-map-sort_nowhite.jsonl || :
