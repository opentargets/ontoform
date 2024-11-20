import sys
from collections.abc import Callable
from enum import Enum
from typing import BinaryIO

import polars as pl
from loguru import logger


class Format(Enum):
    NDJSON = 'ndjson'
    PARQUET = 'parquet'
    TSV = 'tsv'


format_writers = {
    Format.NDJSON: 'write_ndjson',
    Format.PARQUET: 'write_parquet',
    Format.TSV: 'write_csv',
}

format_opts = {
    Format.NDJSON: {},
    Format.PARQUET: {'compression': 'gzip'},  # etl does not support zstd yet
    Format.TSV: {'separator': '\t', 'include_header': False},
}

format_exts = {
    Format.NDJSON: 'jsonl',
    Format.PARQUET: 'parquet',
    Format.TSV: 'tsv',
}


def extension(format: Format) -> str:
    return format_exts[format]


def write_format(data: pl.DataFrame, dst: BinaryIO, format: Format) -> None:
    write_func = None
    try:
        write_func: Callable = getattr(data, format_writers[format])
    except KeyError:
        logger.critical(f'unsupported format: {format}')
        sys.exit(1)

    write_func(dst, **format_opts[format])
