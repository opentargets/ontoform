#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/hpo_phenotypes ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/hpo_phenotypes || exit
mkdir -p ./output

# get hpo_phenotypes
curl -Ls http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa > ./input/phenotype.hpoa

# run the transformation
uv run ontoform hpo_phenotypes ./input/phenotype.hpoa ./output/hpo_phenotypes_ontoform.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./input/hpo_phenotypes_oldpis.jsonl > ./output/hpo_phenotypes_oldpis_sort.jsonl
jq -cf ../../tools/diff.jq ./output/hpo_phenotypes_ontoform.jsonl > ./output/hpo_phenotypes_ontoform_sort.jsonl

# drop the first line of the old PIS output, as it was processing the tsv header
tail -n +2 ./output/hpo_phenotypes_oldpis_sort.jsonl > ./output/hpo_phenotypes_oldpis_sort_noheader.jsonl

# compare the outputs
diff -w --color ./output/hpo_phenotypes_oldpis_sort_noheader.jsonl ./output/hpo_phenotypes_ontoform_sort.jsonl
