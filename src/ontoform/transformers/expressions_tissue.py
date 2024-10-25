from pathlib import Path

import polars as pl
import polars.selectors as cs

def transform(src: Path, dst: Path) -> None:
    
    # load the tissue expressions
    initial = pl.read_json(src).unnest('tissues')
    
    # Get all the column names
    columns = initial.columns
    
    # Create a list of dataframes from the json values and the tissue_id
    tissue_list = [initial
                   .select(tissue=pl.col(column))
                   .unnest('tissue')
                   .with_columns(tissue_id=pl.lit(column)) for column in columns]
    
    # Concatenate the list of dataframes
    output = pl.concat(tissue_list)
    
    # Write the output to the destination
    output.write_ndjson(dst)