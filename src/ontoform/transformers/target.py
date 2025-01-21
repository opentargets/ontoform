import gc
import gzip
import json
import zipfile
from typing import BinaryIO

import polars as pl
from loguru import logger

from ontoform.file_format import FileFormat, write_format
from ontoform.schemas.ensembl import schema


class FilteredJSONDecoder(json.JSONDecoder):
    """JSON Decoder that filter keys in the JSON object.

    This decoder calls a hook on each JSON object that filters out keys not in
    allowed_keys set. It also processes the root_key, so we can get to the data.

    This saves us a lot of memory and time, and serves as a workaround for the
    bug in polars that causes it to crash when loading large JSON objects:

    https://github.com/pola-rs/polars/issues/17677
    """

    def __init__(self, root_key='genes', allowed_keys=None, *args, **kwargs):
        self.root_key = root_key
        self.allowed_keys = allowed_keys or {'id', 'name'}
        super().__init__(*args, **kwargs, object_hook=self.filter_keys)

    def filter_keys(self, obj: dict) -> dict:
        if self.root_key in obj:
            return {self.root_key: obj[self.root_key]}
        return {k: v for k, v in obj.items() if k in self.allowed_keys}


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
            .select([
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
            ])
        )

        logger.debug(f'transformation complete, writing file to {dst.name}')
        write_format(df, dst, output_format)


class GnomadTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        logger.info(f'transforming to gzip, ignoring format argument {output_format.name}')
        with gzip.open(src) as file:
            with gzip.open(dst, 'wb') as gzip_file:
                gzip_file.write(file.read())


class HomologueTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: FileFormat) -> None:
        # load the homologues, parse the json with our decoder, then delete from memory
        data = json.loads(src.read(), cls=FilteredJSONDecoder)
        df = pl.from_dicts(data['genes'])
        del data
        gc.collect()

        # write the result
        write_format(df, dst, output_format)
