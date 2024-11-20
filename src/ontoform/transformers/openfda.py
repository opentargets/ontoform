import zipfile
from pathlib import Path
from typing import BinaryIO

import polars as pl

from ontoform.format import Format, write_format
from ontoform.schemas.openfda import schema


def remove_nulls_from_struct(s):
    return s.struct.field('*').drop_nulls()


class OpenFdaTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: Format) -> None:
        with zipfile.ZipFile(src) as zip_file:
            inner_filename = zip_file.infolist()[0].filename
            df = pl.read_json(zip_file.open(inner_filename), schema=schema)

            results = df.select('results').explode('results').unnest('results')

            write_format(results, dst, output_format)
