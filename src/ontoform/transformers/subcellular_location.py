import gzip
import zipfile
from typing import BinaryIO

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    with zipfile.ZipFile(src, 'r') as zip_file:
        with zip_file.open('subcellular_location.tsv') as file:
            with gzip.open(dst, 'wb') as gzip_file:
                gzip_file.write(file.read())
