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
mkdir -p $work_dir/input/target-inputs/hpa
mkdir -p $work_dir/input/target-inputs/project-scores
mkdir -p $work_dir/input/target-inputs/ensembl
mkdir -p $work_dir/input/target-inputs/gnomad

# get target files

# SubcellularLocationTransformer —
# input/target-inputs/hpa/subcellular_locations.tsv.zip -> input/target-inputs/hpa/subcellular_locations.tsv.gz
curl -Ls https://www.proteinatlas.org/download/tsv/subcellular_location.tsv.zip -o $work_dir/input/target-inputs/hpa/subcellular_location.tsv.zip

# SubcellularLocationSSLTransformer —
# input/target-inputs/hpa/subcellular_locations_ssl.tsv -> target-inputs/hpa/subcellular_locations_ssl.parquet
curl -Ls https://storage.googleapis.com/otar001-core/subcellularLocations/HPA_subcellular_locations_SL-2021-08-19.tsv -o $work_dir/input/target-inputs/hpa/subcellular_locations_ssl.tsv

# EssentialityMatricesTransformer —
# input/target-inputs/project-scores/essentiality_matrices.zip -> input/target-inputs/project-scores/04_binaryDepScores.parquet
curl -Ls https://cog.sanger.ac.uk/cmp/download/essentiality_matrices.zip -o $work_dir/input/target-inputs/project-scores/essentiality_matrices.zip

# GeneIdentifiersTransformer —
# input/target-inputs/project-scores/gene_identifiers_latest.csv.gz -> input/target-inputs/project-scores/gene_identifiers_latest.parquet
curl -Ls https://cog.sanger.ac.uk/cmp/download/gene_identifiers_latest.csv.gz -o $work_dir/input/target-inputs/project-scores/gene_identifiers_latest.csv.gz

# Ensembl — download conditionally
# input/target-inputs/ensembl/homo_sapiens.json -> input/target-inputs/ensembl/homo_sapiens.parquet
if [ ! -f $work_dir/input/target-inputs/ensembl/homo_sapiens.json ]; then
    curl -Ls https://ftp.ensembl.org/pub/release-113/json/homo_sapiens/homo_sapiens.json -o $work_dir/input/target-inputs/ensembl/homo_sapiens.json
fi

# GnomAD —
# input/target-inputs/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz -> input/target-inputs/gnomad/gnomad_lof_by_gene.txt.gz
curl -Ls https://storage.googleapis.com/gcp-public-data--gnomad/release/2.1.1/constraint/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz -o $work_dir/input/target-inputs/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz


# run ontoform
uv run ontoform --work-dir /tmp target --output-format ndjson


# we don't need to check outputs, just knowing everything runs is fine
# except for ensembl, TODO once regular pis runs for release 113
