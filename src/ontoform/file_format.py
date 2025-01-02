import sys
from collections.abc import Callable
from enum import Enum
from typing import BinaryIO

import polars as pl
from loguru import logger


class FileFormat(Enum):
    NDJSON = 'ndjson'
    PARQUET = 'parquet'
    TSV = 'tsv'


file_format_writers = {
    FileFormat.NDJSON: 'write_ndjson',
    FileFormat.PARQUET: 'write_parquet',
    FileFormat.TSV: 'write_csv',
}

file_format_opts = {
    FileFormat.NDJSON: {},
    FileFormat.PARQUET: {'compression': 'gzip'},  # etl does not support zstd yet
    FileFormat.TSV: {'separator': '\t', 'include_header': False},
}

file_format_exts = {
    FileFormat.NDJSON: 'jsonl',
    FileFormat.PARQUET: 'parquet',
    FileFormat.TSV: 'tsv',
}


def stem(file_format: FileFormat) -> str:
    return file_format_exts[file_format]


def write_format(data: pl.DataFrame, dst: BinaryIO, file_format: FileFormat) -> None:
    write_func = None
    try:
        write_func: Callable = getattr(data, file_format_writers[file_format])
    except KeyError:
        logger.critical(f'unsupported file format: {file_format}')
        sys.exit(1)

    write_func(dst, **file_format_opts[file_format])
