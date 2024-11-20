from uuid import uuid4

from ontoform.format import extension
from ontoform.model import GlobTransformation, Step
from ontoform.transformers.openfda import OpenFdaTransformer

openfda = Step(
    name='openfda',
    transformations=[
        GlobTransformation(
            src_prefix='input/fda-inputs',
            dst_path=lambda _, f: f'input/fda-inputs/{uuid4()}.{extension(f)}',
            glob='**/*.zip',
            transformer=OpenFdaTransformer,
        ),
    ],
)
