from pathlib import Path

import polars as pl

def transform(src: Path, dst: Path) -> None:
    
    # load the tissue expressions
    initial = pl.read_json(src).unnest('tissues')
    
    # Unnest every value of the tissues structure and use the key as the column tissue_id
    tissue_list = []
    for key, value in initial.to_dict().items():
        
        # Create a DataFrame with the value and unnest it
        df = pl.DataFrame(value).unnest(key)
        
        # Add tissue_id column with having the key as the value
        df = df.with_columns(pl.lit(key).alias('tissue_id'))
        
        # Append the DataFrame to the list
        tissue_list.append(df)
        
    # The list needs to be concatenated because it's a list of DataFrames
    output = pl.concat(tissue_list)
    
    # Write the output to the destination
    output.write_ndjson(dst)