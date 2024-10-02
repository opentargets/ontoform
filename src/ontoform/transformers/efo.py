from pathlib import Path

import polars as pl

schema = pl.Schema(
    {
        'id': pl.String(),
        'lbl': pl.String(),
        'meta': pl.Struct(
            {
                'basicPropertyValues': pl.List(
                    pl.Struct(
                        {
                            'pred': pl.String(),
                            'val': pl.String(),
                        }
                    )
                ),
                'comments': pl.List(
                    pl.String(),
                ),
                'definition': pl.Struct(
                    {
                        'val': pl.String(),
                        'xrefs': pl.List(
                            pl.String(),
                        ),
                    }
                ),
                'deprecated': pl.Boolean(),
                'subsets': pl.List(
                    pl.String(),
                ),
                'synonyms': pl.List(
                    pl.Struct(
                        {
                            'pred': pl.String(),
                            'synonymType': pl.String(),
                            'val': pl.String(),
                            'xrefs': pl.List(
                                pl.String(),
                            ),
                        },
                    ),
                ),
                'xrefs': pl.List(
                    pl.Struct(
                        {
                            'val': pl.String(),
                        }
                    )
                ),
            }
        ),
        'propertyType': pl.String(),
        'type': pl.String(),
    }
)


def transform(src: Path, dst: Path) -> None:
    # load the ontology
    initial = pl.read_json(src)

    # prepare edge data
    e0 = pl.DataFrame(initial['graphs'][0][0]['edges']).filter(
        pl.col('pred') == 'is_a',
    )
    e = e0.with_columns(
        sub=pl.col('sub').str.split('/').list.last(),
        obj=pl.col('obj').str.split('/').list.last(),
    )

    # prepare location data
    o0 = pl.DataFrame(initial['graphs'][0][0]['edges']).filter(
        pl.col('pred') == 'http://purl.obolibrary.org/obo/BFO_0000050',
    )
    o = o0.with_columns(
        sub=pl.col('sub').str.split('/').list.last(),
        obj=pl.col('obj').str.split('/').list.last(),
    )

    # prepare node data
    n_base = pl.DataFrame(
        initial['graphs'][0][0]['nodes'],
        strict=False,
        schema=schema,
    )

    # cleanup and renaming
    n0 = (
        n_base.filter(
            pl.col('type') == 'CLASS',
            ~pl.col('meta').struct['deprecated'] | pl.col('meta').struct['deprecated'].is_null(),
        )
        .unnest('meta')
        .with_columns(
            id=pl.col('id').str.split('/').list.last(),
            code=pl.col('id'),
            isTherapeuticArea=pl.col('subsets').list.contains('"therapeutic_area"'),
            definition=pl.col('definition').struct['val'],
            dbXRefs=pl.col('xrefs').list.eval(pl.element().struct.field('val').unique()),
        )
        .drop(
            'basicPropertyValues',
            'comments',
            'deprecated',
            'propertyType',
            'subsets',
            'type',
            'xrefs',
        )
        .rename({'lbl': 'label'})
    )

    # parents
    agg_columns = ['code', 'label', 'definition', 'isTherapeuticArea', 'dbXRefs', 'synonyms']
    n1_a = n0.join(e, left_on='id', right_on='sub', how='left').rename({'obj': 'parents'})
    n1 = n1_a.group_by('id').agg(*[pl.col(col).first() for col in agg_columns], pl.col('parents').drop_nulls())

    # locationIds
    agg_columns.append('parents')
    n2_a = n1.join(o, left_on='id', right_on='sub', how='left').rename({'obj': 'locationIds'})
    n2 = n2_a.group_by('id').agg(*[pl.col(col).first() for col in agg_columns], pl.col('locationIds').drop_nulls())

    # fix synonyms
    synonym_columns = ['hasExactSynonym', 'hasRelatedSynonym', 'hasNarrowSynonym', 'hasBroadSynonym']
    n3_a = (
        (
            n2['id', 'synonyms']
            .explode('synonyms')
            .filter(pl.col('synonyms').struct['pred'].is_in(synonym_columns))
            .unnest('synonyms')
            .with_columns(val=pl.col('val').str.replace_all('\n', '').str.strip_chars())
            .group_by(['id', 'pred'])
            .agg(pl.col('val').unique())
            .pivot(values='val', index='id', columns='pred', aggregate_function='first')
        )
        .with_columns([pl.col(col).fill_null([]) for col in synonym_columns])
        .with_columns(
            synonyms=pl.struct(
                hasExactSynonym=pl.col('hasExactSynonym'),
                hasRelatedSynonym=pl.col('hasRelatedSynonym'),
                hasNarrowSynonym=pl.col('hasNarrowSynonym'),
                hasBroadSynonym=pl.col('hasBroadSynonym'),
            )
        )
        .select(['id', 'synonyms'])
    )
    n3 = n2.drop('synonyms').join(n3_a, on='id', how='left')

    # compute obsolete related entities
    n4_a = (
        n_base.filter(
            pl.col('meta').struct['deprecated']
            & pl.col('meta')
            .struct['basicPropertyValues']
            .list.eval(pl.element().struct.field('pred') == 'http://purl.obolibrary.org/obo/IAO_0100001')
            .list.any()
        )
        .unnest('meta')
        .explode('basicPropertyValues')
        .unnest('basicPropertyValues')
        .filter(pl.col('pred') == 'http://purl.obolibrary.org/obo/IAO_0100001')
        .select(['id', 'val'])
        .rename({'id': 'obsoleteTerms', 'val': 'code'})
    )

    agg_columns.append('locationIds')
    n4 = (
        n3.join(n4_a, on='code', how='left')
        .group_by('id')
        .agg(*[pl.col(col).first() for col in agg_columns], pl.col('obsoleteTerms'))
    ).with_columns(obsoleteTerms=pl.col('obsoleteTerms').list.eval(pl.element().str.split('/').list.last()))

    # write the result
    n4.write_ndjson(dst)
