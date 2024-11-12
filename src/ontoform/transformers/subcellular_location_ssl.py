from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats, write_dst


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    df = pl.read_csv(src, has_header=True, separator='\t')

    write_dst(df, dst, format)
