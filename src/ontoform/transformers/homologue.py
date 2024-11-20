import gc
import json
from typing import BinaryIO

import polars as pl

from ontoform.format import Format, write_format


class FilteredJSONDecoder(json.JSONDecoder):
    """JSON Decoder that filter keys in the JSON object.

    This decoder calls a hook on each JSON object that filters out keys not in
    allowed_keys set. It also processes the root_key, so we can get to the data.

    This saves us a lot of memory and time, and serves as a workaround for the
    bug in polars that causes it to crash when loading large JSON objects:

    https://github.com/pola-rs/polars/issues/17677
    """

    def __init__(self, root_key='genes', allowed_keys=None, *args, **kwargs):
        self.root_key = root_key
        self.allowed_keys = allowed_keys or {'id', 'name'}
        super().__init__(*args, **kwargs, object_hook=self.filter_keys)

    def filter_keys(self, obj: dict) -> dict:
        if self.root_key in obj:
            return {self.root_key: obj[self.root_key]}
        return {k: v for k, v in obj.items() if k in self.allowed_keys}


class HomologueTransformer:
    def transform(self, src: BinaryIO, dst: BinaryIO, output_format: Format) -> None:
        # load the homologues, parse the json with our decoder, then delete from memory
        data = json.loads(src.read(), cls=FilteredJSONDecoder)
        df = pl.from_dicts(data['genes'])
        del data
        gc.collect()

        # write the result
        write_format(df, dst, output_format)
