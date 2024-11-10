from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats, ontology_schema, write_dst


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    # Reads the so ontology
    initial = pl.read_json(src)

    # prepare node data
    node_list = pl.DataFrame(
        initial['graphs'][0][0]['nodes'],
        schema=ontology_schema,
        strict=False,
    ).filter(
        pl.col('type') == 'CLASS',
    )

    # filter out non so terms and terms without labels, then select the id and label columns
    output = node_list.filter(
        pl.col('id').str.contains('SO_'),
        pl.col('lbl').is_not_null(),
    ).select(
        id=pl.col('id').str.split('/').list.last().str.replace('_', ':'),
        label=pl.col('lbl'),
    )

    # write the result
    write_dst(output, dst, format)
