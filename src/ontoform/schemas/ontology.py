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
                        },
                    ),
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
                    },
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
                        },
                    ),
                ),
            },
        ),
        'type': pl.String(),
    }
)
