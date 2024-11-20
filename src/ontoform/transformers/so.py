from typing import BinaryIO

import polars as pl

from ontoform.format import Format, write_format
from ontoform.schemas.ontology import schema


class SOTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: Format) -> None:
        # load the ontology
        initial = pl.read_json(src)

        # prepare node data
        node_list = pl.DataFrame(
            initial['graphs'][0][0]['nodes'],
            schema=schema,
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
        write_format(output, dst, output_format)
