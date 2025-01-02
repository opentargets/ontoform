from uuid import uuid4

from ontoform.file_format import stem
from ontoform.model import GlobTransformation, Step
from ontoform.transformers.openfda import OpenFdaTransformer

openfda = Step(
    name='openfda',
    transformations=[
        GlobTransformation(
            src_prefix='input/fda-inputs',
            dst_path=lambda _, f: f'input/fda-inputs/{uuid4()}.{stem(f)}',
            glob='**/*.zip',
            transformer=OpenFdaTransformer,
        ),
    ],
)
