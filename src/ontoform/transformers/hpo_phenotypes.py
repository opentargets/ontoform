from pathlib import Path

import polars as pl


def transform(src: Path, dst: Path) -> None:
    # load the ontology
    initial = pl.read_csv(src, separator='\t', comment_prefix='#')

    n1 = pl.DataFrame().with_columns(
        databaseId=initial['database_id'],
        diseaseName=initial['disease_name'],
        qualifier=initial['qualifier'],
        HPOId=initial['hpo_id'],
        references=initial['reference'].str.split(';').list.unique(),
        evidenceType=initial['evidence'],
        onset=initial['onset'].str.replace_all('HP:', 'HP_').str.split(';').list.unique(),
        frequency=initial['frequency'].str.replace_all('HP:', 'HP_'),
        sex=initial['sex'],
        modifiers=initial['modifier'].str.replace_all('HP:', 'HP_').str.split(';').list.unique(),
        aspect=initial['aspect'],
        biocuration=initial['biocuration'].str.strip_chars_end(),
        resource=pl.lit('HPO'),
    )

    # write the result
    n1.write_ndjson(dst)
