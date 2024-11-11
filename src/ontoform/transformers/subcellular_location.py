import gzip
import zipfile
from typing import BinaryIO

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    with zipfile.ZipFile(src, 'r') as zip_file:
        for item in zip_file.filelist:
            with zip_file.open(item) as file:
                if item.filename == 'subcellular_location.tsv':
                    with gzip.open(dst, 'wb') as gzip_file:
                        gzip_file.write(file.read())