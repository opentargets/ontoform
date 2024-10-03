#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/efo_otar_slim ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/efo_otar_slim || exit
mkdir -p ./output

# get efo
curl -Ls https://github.com/EBISPOT/efo/releases/download/v3.70.0/efo_otar_slim.json > ./input/efo_otar_slim.json

# run the transformation
uv run ontoform efo ./input/efo_otar_slim.json ./output/efo_otar_slim_ontoform.jsonl

# drop definitions, we already know they will be different
# and the definition alternatives, which are not there in the ontoform output
# also, sort rows
jq -c 'del(.definition)|del(.definition_alternatives)' ./input/ontology_efo-oldpis.jsonl | sort > ./output/oldpis_nodef.jsonl
jq -c 'del(.definition)|del(.definition_alternatives)' ./output/efo_otar_slim_ontoform.jsonl | sort > ./output/ontoform_nodef.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./output/oldpis_nodef.jsonl > ./output/oldpis_nodef_trim.jsonl
jq -cf ../../tools/diff.jq ./output/ontoform_nodef.jsonl > ./output/ontoform_nodef_trim.jsonl

# sort the object keys
jq -s "." ./output/oldpis_nodef_trim.jsonl | jq --sort-keys "." | jq -c ".[]" > ./output/oldpis_nodef_trim_sort.jsonl
jq -s "." ./output/ontoform_nodef_trim.jsonl | jq --sort-keys "." | jq -c ".[]" > ./output/ontoform_nodef_trim_sort.jsonl

# remove nulls from the ontoform output
jq -c "del(..|nulls)" ./output/ontoform_nodef_trim_sort.jsonl > ./output/ontoform_nodef_trim_sort_nonulls.jsonl

# remove empty arrays
jq -c "del(.. | select(. == []))" ./output/oldpis_nodef_trim_sort.jsonl > ./output/oldpis_nodef_trim_sort_noempty.jsonl
jq -c "del(.. | select(. == []))" ./output/ontoform_nodef_trim_sort_nonulls.jsonl > ./output/ontoform_nodef_trim_sort_nonulls_noempty.jsonl

# compare the outputs
diff --color ./output/oldpis_nodef_trim_sort_noempty.jsonl ./output/ontoform_nodef_trim_sort_nonulls_noempty.jsonl || :

# disease file comparison
# sort diseases object keys and rows
jq -s "." ./input/diseases_efo-oldpis.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/oldpis_diseases_sort.jsonl
jq -s "." ./output/efo_otar_slim_ontoform_diseases.jsonl | jq --sort-keys "." | jq -c ".[]" | sort > ./output/efo_otar_slim_ontoform_diseases_sort.jsonl

# sort the arrays
jq -cf ../../tools/diff.jq ./output/oldpis_diseases_sort.jsonl > ./output/oldpis_diseases_sort_arrays.jsonl
jq -cf ../../tools/diff.jq ./output/efo_otar_slim_ontoform_diseases_sort.jsonl > ./output/efo_otar_slim_ontoform_diseases_sort_arrays.jsonl

# compare the outputs
diff --color ./output/oldpis_diseases_sort_arrays.jsonl ./output/efo_otar_slim_ontoform_diseases_sort_arrays.jsonl || :
