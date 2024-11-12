import gzip
import zipfile
from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    with zipfile.ZipFile(src) as zip_file:
        with zip_file.open('normal_tissue.tsv') as file:
            with gzip.open(dst, 'wb') as gzip_file:
                gzip_file.write(file.read())
