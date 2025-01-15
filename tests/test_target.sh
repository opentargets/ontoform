#!/bin/bash

# This script is meant to be run from the root of the repository
if [ ! -d ./tests ]; then
    echo "Please run this script from the root of the repository (like: ./tests/$(basename "$0"))"
    exit 1
fi

work_dir=./work # /tmp might be too small if it uses tmpfsgg

# set up stuff
set -x
set -e
mkdir -p $work_dir/input/target/hpa
mkdir -p $work_dir/input/target/project-scores
mkdir -p $work_dir/input/target/ensembl
mkdir -p $work_dir/input/target/gnomad
mkdir -p $work_dir/input/target/homologue/gene_dictionary

# get target files

# SubcellularLocationTransformer —
# input/target/hpa/subcellular_locations.tsv.zip -> intermediate/target/hpa/subcellular_locations.tsv.gz
curl -Ls https://www.proteinatlas.org/download/tsv/subcellular_location.tsv.zip -o $work_dir/input/target/hpa/subcellular_location.tsv.zip

# SubcellularLocationSSLTransformer —
# input/target/hpa/subcellular_locations_ssl.tsv -> intermediate/target/hpa/subcellular_locations_ssl.parquet
curl -Ls https://storage.googleapis.com/otar001-core/subcellularLocations/HPA_subcellular_locations_SL-2021-08-19.tsv -o $work_dir/input/target/hpa/subcellular_locations_ssl.tsv

# EssentialityMatricesTransformer —
# input/target/project-scores/essentiality_matrices.zip -> intermediate/target/project-scores/04_binaryDepScores.parquet
curl -Ls https://cog.sanger.ac.uk/cmp/download/essentiality_matrices.zip -o $work_dir/input/target/project-scores/essentiality_matrices.zip

# GeneIdentifiersTransformer —
# input/target/project-scores/gene_identifiers_latest.csv.gz -> intermediate/target/project-scores/gene_identifiers_latest.parquet
curl -Ls https://cog.sanger.ac.uk/cmp/download/gene_identifiers_latest.csv.gz -o $work_dir/input/target/project-scores/gene_identifiers_latest.csv.gz

# GnomAD —
# input/target/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz -> intermediate/target/gnomad/gnomad_lof_by_gene.txt.gz
curl -Ls https://storage.googleapis.com/gcp-public-data--gnomad/release/2.1.1/constraint/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz -o $work_dir/input/target/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz

# Ensembl — download conditionally
# input/target/ensembl/homo_sapiens.json -> intermediate/target/ensembl/homo_sapiens.parquet
if [ ! -f $work_dir/input/target/ensembl/homo_sapiens.json ]; then
    curl -Ls https://ftp.ensembl.org/pub/release-113/json/homo_sapiens/homo_sapiens.json -o $work_dir/input/target/ensembl/homo_sapiens.json
fi
# homologue — input/target/homologue/gene_dictionary/homo_sapiens.json -> intermediate/target/homologue/gene_dictionary/homo_sapiens.parquet
cp $work_dir/input/target/ensembl/homo_sapiens.json $work_dir/input/target/homologue/gene_dictionary/homo_sapiens.json
gsutil cp gs://open-targets-pre-data-releases/24.12-uo_test-3/input/target-inputs/homologue/gene_dictionary/homo_sapiens.parquet $work_dir/previous_homologue.parquet

# run the target transformation
uv run ontoform --work-dir $work_dir target

# compare the outputs
diff $work_dir/previous_homologue.parquet $work_dir/intermediate/target/homologue/gene_dictionary/homo_sapiens.parquet || :

# we don't need to check other outputs (those just convert formats), just knowing everything runs is fine
