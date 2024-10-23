from pathlib import Path

import polars as pl

def transform(src: Path, dst: Path) -> None:
    # load the homologues
    initial = pl.read_json(src)

    # prepare node data
    inputGenes = pl.DataFrame(
        initial['genes']
    )

    # extract genes list
    genes_list = inputGenes.explode('genes').unnest('genes')

    # read id and name
    output = genes_list.select(["id","name"])

    # write the result
    output.write_ndjson(dst)
