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
    
    
    # prepare node data
    node_list = pl.DataFrame(
        initial['graphs'][0][0]['nodes'],
        strict=False,
        schema=schema,
    ).filter(
        pl.col('type') == 'CLASS',
    )
    
    # write the result
    node_list.write_ndjson(dst)