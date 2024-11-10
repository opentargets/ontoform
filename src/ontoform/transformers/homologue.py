from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats, write_dst


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    # load the homologues
    initial = pl.read_json(src)

    # prepare node data
    input_genes = pl.DataFrame(initial['genes'])

    # extract genes list
    genes_list = input_genes.explode('genes').unnest('genes').select(['id', 'name'])

    # write the result
    write_dst(genes_list, dst, format)
