import sys
from collections.abc import Callable
from enum import Enum
from typing import BinaryIO

import polars as pl
from loguru import logger


class SupportedFormats(Enum):
    NDJSON = 'ndjson'
    PARQUET = 'parquet'
    TSV = 'tsv'


format_writers = {
    SupportedFormats.NDJSON: 'write_ndjson',
    SupportedFormats.PARQUET: 'write_parquet',
    SupportedFormats.TSV: 'write_csv',
}

format_opts = {
    SupportedFormats.NDJSON: {},
    SupportedFormats.PARQUET: {'compression': 'gzip'},
    SupportedFormats.TSV: {'separator': '\t', 'include_header': False},
}


def write_dst(data: pl.DataFrame, dst: BinaryIO, format: SupportedFormats) -> None:
    write_func = None
    try:
        write_func: Callable = getattr(data, format_writers[format])
    except KeyError:
        logger.critical(f'unsupported format: {format}')
        sys.exit(1)

    write_func(dst, **format_opts[format])


ontology_schema = pl.Schema(
    {
        'id': pl.String(),
        'lbl': pl.String(),
        'meta': pl.Struct(
            {
                'basicPropertyValues': pl.List(
                    pl.Struct(
                        {
                            'pred': pl.String(),
                            'val': pl.String(),
                        },
                    ),
                ),
                'comments': pl.List(
                    pl.String(),
                ),
                'definition': pl.Struct(
                    {
                        'val': pl.String(),
                        'xrefs': pl.List(
                            pl.String(),
                        ),
                    },
                ),
                'deprecated': pl.Boolean(),
                'subsets': pl.List(
                    pl.String(),
                ),
                'synonyms': pl.List(
                    pl.Struct(
                        {
                            'pred': pl.String(),
                            'synonymType': pl.String(),
                            'val': pl.String(),
                            'xrefs': pl.List(
                                pl.String(),
                            ),
                        },
                    ),
                ),
                'xrefs': pl.List(
                    pl.Struct(
                        {
                            'val': pl.String(),
                        },
                    ),
                ),
            },
        ),
        'type': pl.String(),
    }
)
