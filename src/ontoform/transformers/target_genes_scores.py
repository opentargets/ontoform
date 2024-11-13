from typing import BinaryIO
import zipfile
import polars as pl

from ontoform.util import SupportedFormats, write_dst

def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    with zipfile.ZipFile(src) as zip_file:
        with zip_file.open('EssentialityMatrices/04_binaryDepScores.tsv') as file:
            df = pl.read_csv(file, separator='\t')
            write_dst(df, dst, format)
