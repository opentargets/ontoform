import gzip
from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats, write_dst


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    with gzip.open(src, 'rb') as gzip_file:
        df = pl.read_csv(gzip_file.read())
        write_dst(df, dst, format)
