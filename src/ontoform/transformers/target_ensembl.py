from typing import BinaryIO

import polars as pl

from ontoform.util import SupportedFormats, write_dst


def transform(src: BinaryIO, dst: BinaryIO, format: SupportedFormats) -> None:
    initial = pl.read_json(src)
    df = pl.DataFrame(initial['genes'], strict=False).explode('genes').unnest('genes')

    selected = df.select(
        [
            'id',
            'biotype',
            'description',
            'end',
            'start',
            'strand',
            'seq_region_name',
            'name',
            'transcripts',
            'SignalP',
            'Uniprot/SPTREMBL',
            'Uniprot/SWISSPROT',
        ]
    )
    output = selected.rename(
        {
            'seq_region_name': 'chromosome',
            'name': 'approvedSymbol',
            'Uniprot/SPTREMBL': 'uniprot_trembl',
            'Uniprot/SWISSPROT': 'uniprot_swissprot',
        }
    )

    write_dst(output, dst, format)
