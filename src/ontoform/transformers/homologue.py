from pathlib import Path

import polars as pl

# Define schema with the fields that are going to be used

schema = pl.Schema(
    {
        'id': pl.String(),
        'name': pl.String(),
    }
)


def transform(src: Path, dst: Path) -> None:
    # load the homologues
    initial = pl.read_json(src)

    # prepare node data
    inputGenes = pl.DataFrame(
        initial['genes']
    )


    # head = inputGenes.head()

    genes_list = inputGenes.explode('genes').unnest('genes')

    print(genes_list.schema)
    output = genes_list.select(["id","name"])
    # print(output.p())

    # write the result
    output.write_ndjson(dst)
