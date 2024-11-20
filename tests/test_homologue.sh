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
mkdir -p $work_dir/input/target-inputs/homologue/gene-dictionary

# get a couple of homologue files, conditionally
# HomologueTransformer
# input/target-inputs/homologue/gene-dictionary/**/*.json -> input/target-inputs/homologue/gene-dictionary-transformed/*.parquet
if [ ! -f $work_dir/input/target-inputs/homologue/gene-dictionary/homo_sapiens.json ]; then
    curl -Ls https://ftp.ensembl.org/pub/release-112/json/homo_sapiens/homo_sapiens.json > $work_dir/input/target-inputs/homologue/gene-dictionary/homo_sapiens.json
fi
gsutil cp gs://open-targets-pre-data-releases/24.09dev/input/target-inputs/homologue/gene_dictionary/homo_sapiens.tsv $work_dir/oldpis-homologue.tsv

if [ ! -f $work_dir/input/target-inputs/homologue/gene-dictionary/mus_musculus.json ]; then
    curl -Ls https://ftp.ensembl.org/pub/release-112/json/mus_musculus/mus_musculus.json > $work_dir/input/target-inputs/homologue/gene-dictionary/mus_musculus.json
fi

# run the transformation
uv run ontoform --work-dir /tmp homologue --output-format tsv

# we'll just check homo_sapiens, as it is the biggest one of them, but make sure mus_musculus was generated too
[ ! -f $work_dir/input/target-inputs/homologue/gene-dictionary-transformed/mus_musculus.tsv ] && echo "mus_musculus was not generated!"

# fix the escaping in old pis
sed 's/"//g; s/\\t/\t/g' $work_dir/oldpis-homologue.tsv > $work_dir/oldpis-homologue-fixed.tsv

# sort rows
sort < $work_dir/oldpis-homologue-fixed.tsv > $work_dir/oldpis-homologue-fixed_sort.tsv
sort < $work_dir/input/target-inputs/homologue/gene-dictionary-transformed/homo_sapiens.tsv > $work_dir/ontoform-homologue-sort.tsv

# compare the outputs
diff --color $work_dir/oldpis-homologue-fixed_sort.tsv $work_dir/ontoform-homologue-sort.tsv || :
