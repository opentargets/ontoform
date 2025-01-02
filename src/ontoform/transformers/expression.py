import gzip
import zipfile
from typing import BinaryIO

import polars as pl
from loguru import logger

from ontoform.file_format import FileFormat, write_format


class NormalTissueTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        logger.info(f'transforming to gzip, ignoring format argument {output_format.name}')
        with zipfile.ZipFile(src) as zip_file:
            with zip_file.open('normal_tissue.tsv') as file:
                with gzip.open(dst, 'wb') as gzip_file:
                    gzip_file.write(file.read())


class TissueTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        initial = pl.read_json(src).unnest('tissues')

        columns = initial.columns
        tissue_list = [
            initial.select(tissue=pl.col(column)).unnest('tissue').with_columns(tissue_id=pl.lit(column))
            for column in columns
        ]
        output = pl.concat(tissue_list)

        write_format(output, dst, output_format)
