from pathlib import Path

import polars as pl

schema = pl.Schema({     
          "id": pl.String(),
          "lbl": pl.String(),
          "meta": pl.Struct({
            "basicPropertyValues": 
              pl.list(pl.Struct({
                "pred": pl.String(),
                "val": pl.String()
              }))
            ,
            "comments": pl.list(pl.String()),
            "definition": pl.Struct({
              "val": pl.String(),
              "xrefs": pl.list(pl.String())
            }),
            "deprecated": pl.Boolean(),
            "subsets": pl.list(pl.String()),
            "synonyms": 
              pl.list(pl.Struct({
                "pred": pl.String(),
                "synonymType": pl.String(),
                "val": pl.String(),
                "xrefs": pl.list(pl.String())
              })),
            "xrefs": 
              pl.list(pl.Struct({
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
        pl.col('type') != 'PROPERTY',
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
        pl.col('type') != 'CLASS',
    )

    pass
