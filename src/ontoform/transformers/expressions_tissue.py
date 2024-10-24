from pathlib import Path

import polars as pl

def transform(src: Path, dst: Path) -> None:
    
    # load the tissue expressions
    initial = pl.read_json(src).unnest('tissues')
    
    # Unnest the tissues and use the key as a column
    tissue_list = []
    for key, value in initial.to_dict().items():
        
        
        df = pl.DataFrame(value).unnest(key)
        
        df = df.with_columns(pl.lit(key).alias('tissue_id'))
        
        tissue_list.append(df)
        
    # The list needs to be concatenated because it's a list of DataFrames
    output = pl.concat(tissue_list)
    
    output.write_ndjson(dst)