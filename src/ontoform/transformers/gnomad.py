import gzip
from typing import BinaryIO

from loguru import logger

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    """Take for e.g. a bgzipped file and write it to a gzip file."""
    logger.info(
        f'transforming to gzip, ignoring format argument {format.name}'
    )
    with gzip.open(src) as file:
        with gzip.open(dst, 'wb') as gzip_file:
            gzip_file.write(file.read())
