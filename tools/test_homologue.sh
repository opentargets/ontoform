#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./examples/homologue ]; then
    echo "Please run this script from the root of the repository (like: ./tools/$(basename "$0"))"
    exit 1
fi

# set up stuff
set -x
set -e
cd ./examples/homologue || exit
mkdir -p ./output

# get homo sapiens homologues, conditionally
if [ ! -f ./input/homo_sapiens.json ]; then
    curl -Ls https://ftp.ensembl.org/pub/release-111/json/homo_sapiens/homo_sapiens.json > ./input/homo_sapiens.json
fi

# run the transformation
uv run ontoform homologue ./input/homo_sapiens.json ./output/ontoform-homologue.tsv --format tsv

# fix the escaping in old pis
sed 's/"//g; s/\\t/\t/g' ./input/oldpis-homologue.tsv > ./output/oldpis-homologue-fixed.tsv

# sort rows
sort < ./output/oldpis-homologue-fixed.tsv    > ./output/oldpis-homologue-fixed-sort.tsv
sort < ./output/ontoform-homologue.tsv > ./output/ontoform-homologue-sort.tsv

# compare the outputs
diff --color ./output/oldpis-homologue-fixed-sort.tsv ./output/ontoform-homologue-sort.tsv || :
