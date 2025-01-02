import gzip
import zipfile
from typing import BinaryIO

import polars as pl
from loguru import logger

from ontoform.file_format import FileFormat, write_format
from ontoform.schemas.ensembl import schema


class SubcellularLocationTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        logger.info(f'transforming to gzip, ignoring format argument {output_format.name}')
        with zipfile.ZipFile(src) as zip_file:
            with zip_file.open('subcellular_location.tsv') as file:
                with gzip.open(dst, 'wb') as gzip_file:
                    gzip_file.write(file.read())


class SubcellularLocationSSLTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        # just change output format
        df = pl.read_csv(src, has_header=True, separator='\t')
        write_format(df, dst, output_format)


class EssentialityMatricesTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        with zipfile.ZipFile(src) as zip_file:
            with zip_file.open('EssentialityMatrices/04_binaryDepScores.tsv') as file:
                df = pl.read_csv(file, separator='\t')
                write_format(df, dst, output_format)


class GeneIdentifiersTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        df = pl.read_csv(src)
        write_format(df, dst, output_format)


class EnsemblTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        df = (
            pl.read_json(src, schema=schema)
            .drop('id')
            .explode('genes')
            .unnest('genes')
            .select(
                [
                    'id',
                    'biotype',
                    'description',
                    'end',
                    'start',
                    'strand',
                    pl.col('seq_region_name').alias('chromosome'),
                    pl.col('name').alias('approvedSymbol'),
                    'transcripts',
                    'SignalP',
                    pl.col('Uniprot/SPTREMBL').alias('uniprot_trembl'),
                    pl.col('Uniprot/SWISSPROT').alias('uniprot_swissprot'),
                ]
            )
        )

        logger.debug(f'transformation complete, writing file to {dst.name}')
        write_format(df, dst, output_format)


class GnomadTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        logger.info(f'transforming to gzip, ignoring format argument {output_format.name}')
        with gzip.open(src) as file:
            with gzip.open(dst, 'wb') as gzip_file:
                gzip_file.write(file.read())
