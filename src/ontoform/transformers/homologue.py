import gc
import ijson
from typing import BinaryIO, Generator

import polars as pl

from ontoform.util import SupportedFormats, write_dst


def load_data(
    src: bytes, root_key="genes", allowed_keys={"id", "name"}
) -> Generator[dict, None, None]:
    """generator to load the homologues from the source file"""
    genes = ijson.items(src, f"{root_key}.item")
    for gene in genes:
        yield {k: gene[k] for k in allowed_keys if k in gene}


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    # load the homologues and delete the enormous dict from memory
    data = load_data(src.read())
    df = pl.DataFrame(data)
    del data
    gc.collect()

    # write the result
    write_dst(df, dst, format)
