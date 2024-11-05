import zipfile
from typing import BinaryIO

import polars as pl


def transform(src: BinaryIO, dst: BinaryIO) -> None:
    with zipfile.ZipFile(src, 'r') as zip_file:
        for item in zip_file.filelist:
            with zip_file.open(item) as file:
                initial = pl.read_json(file)
                results = initial.select('results').explode('results').unnest('results')
                results.write_parquet(dst)
