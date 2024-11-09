import sys
from collections.abc import Callable
from enum import Enum
from typing import BinaryIO

import polars as pl
from loguru import logger


class SupportedFormats(Enum):
    PARQUET = 'parquet'
    NDJSON = 'ndjson'


options = {
    SupportedFormats.PARQUET: {'compression': 'zstd'},
    SupportedFormats.NDJSON: {},
}


def write_dst(data: pl.DataFrame, dst: BinaryIO, format: SupportedFormats) -> None:
    write_func = None
    try:
        write_func: Callable = getattr(data, f'write_{format.value}')
    except KeyError:
        logger.critical(f'unsupported format: {format}')
        sys.exit(1)

    write_func(dst, **options[format])
