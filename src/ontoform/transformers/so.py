from pathlib import Path

import polars as pl

schema = pl.Schema({     
          "id": pl.String(),
          "lbl": pl.String(),
          "meta": pl.Struct({
            "basicPropertyValues": 
              pl.List(pl.Struct({
                "pred": pl.String(),
                "val": pl.String()
              }))
            ,
            "comments": pl.List(pl.String()),
            "definition": pl.Struct({
              "val": pl.String(),
              "xrefs": pl.List(pl.String())
            }),
            "deprecated": pl.Boolean(),
            "subsets": pl.List(pl.String()),
            "synonyms": 
              pl.List(pl.Struct({
                "pred": pl.String(),
                "synonymType": pl.String(),
                "val": pl.String(),
                "xrefs": pl.List(pl.String())
              })),
            "xrefs": 
              pl.List(pl.Struct({
                "val": pl.String()
              }))
          }),
          "type": pl.String()
        })

def transform(src: Path, dst: Path) -> None:
    # Reads the so owl file
    initial = pl.read_json(src)

    # prepare edge data this will be used to create isSubclassOf
    subclasses_list = pl.DataFrame(initial['graphs'][0][0]['edges']).filter(
        pl.col('pred') == 'is_a',
    )
    subclasses_values = subclasses_list.with_columns(
        sub=pl.col('sub').str.split('/').list.last(),
        obj=pl.col('obj').str.split('/').list.last(),
    )
    
    # prepare property data
    property_list = pl.DataFrame(initial['graphs'][0][0]['nodes']).filter(
        (pl.col('type') == 'PROPERTY') & pl.col('meta').is_null() & pl.col('id').str.contains('formats').not_(),
    )
    
    property_values = property_list.with_columns(
        id=pl.col('id').str.split('/').list.last(),
    )
    
    # prepare node data
    node_list = pl.DataFrame(
        initial['graphs'][0][0]['nodes'],
        strict=False,
        schema=schema,
    ).filter(
        pl.col('type') == 'CLASS',
    )
    
    # cleanup and renaming
    cleaned_node_list = (
        node_list.filter(
            pl.col('type') == 'CLASS'
        )
        .unnest('meta')
        .with_columns(
            id_at='obo:'+pl.col('id').str.split('/').list.last(),
            id=''+pl.col('id').str.split('/').list.last().str.replace('_', ':'),
            definition=pl.col('definition').struct['val'],
            dbXRefs=pl.col('xrefs').list.eval(pl.element().struct.field('val').unique()),
            type='owl:'+pl.col('type').str.to_lowercase().str.replace('c', 'C'),
        )
        .drop(
            'basicPropertyValues',
            'comments',
            'subsets',
            'xrefs',
        )
        .rename({'lbl': 'label', 'id_at': '@id'})
    )
    
    # fix synonyms
    synonym_columns = ['hasExactSynonym', 'hasRelatedSynonym', 'hasNarrowSynonym', 'hasBroadSynonym']
    synonyms_list = (
        (
            cleaned_node_list['id', 'synonyms']
            .explode('synonyms')
            .filter(pl.col('synonyms').struct['pred'].is_in(synonym_columns))
            .unnest('synonyms')
            .with_columns(val=pl.col('val').str.replace_all('\n', '').str.strip_chars())
            .group_by(['id', 'pred'])
            .agg(pl.col('val').unique())
            .pivot(values='val', index='id', columns='pred', aggregate_function='first')
        )
    )
    nodes_with_synonyms = cleaned_node_list.drop('synonyms').join(synonyms_list, on='id', how='left')
    
    # compute obsolete related entities
    properties = (
        node_list
        .with_columns(
            id=''+pl.col('id').str.split('/').list.last().str.replace('_', ':'))
        .unnest('meta')
        .explode('basicPropertyValues')
        .unnest('basicPropertyValues')
        .select(['id', 'val', 'pred'])
        .with_columns(pred=pl.col('pred').str.split('#').list.last())
        .with_columns(pl.when(pl.col('pred').str.contains('/')).then(pl.col('pred').str.split("/").list.last()).otherwise(pl.col('pred')).alias('pred'))
        # .pivot(values='val', index='id', columns='pred', aggregate_function='first')
    )
    
    properties.write_ndjson('/home/ricardo/Documents/work-files/pis/props.json')
    
    # merge the properties with the nodes
    
    # write the result
    nodes_with_synonyms.write_ndjson(dst)