import gzip
from typing import BinaryIO

from loguru import logger

from ontoform.util import SupportedFormats


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    """Take for e.g. a bgzipped file and write it to a gzip file."""
    logger.info(
        f"transforming {src.name} to {dst.name}, ignoring format: {format.name}"
    )
    with gzip.open(src, "rb") as file:
        with gzip.open(dst, "wb") as gzip_file:
            gzip_file.write(file.read())
