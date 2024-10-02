from pathlib import Path

import polars as pl

schema = pl.Schema({'id': pl.String()})


def transform(src: Path, dst: Path) -> None:
    pass
