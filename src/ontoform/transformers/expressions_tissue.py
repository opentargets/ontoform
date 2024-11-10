from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    # load the tissue expressions
    initial = pl.read_json(src).unnest('tissues')

    # get all the column names
    columns = initial.columns

    # create a list of dataframes from the json values and the tissue_id
    tissue_list = [
        initial.select(tissue=pl.col(column)).unnest('tissue').with_columns(tissue_id=pl.lit(column))
        for column in columns
    ]

    # concatenate the list of dataframes
    output = pl.concat(tissue_list)

    # write the output to the destination
    output.write_ndjson(dst)
