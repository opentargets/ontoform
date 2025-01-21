from ontoform.file_format import stem
from ontoform.model import FileTransformation, Step
from ontoform.transformers.so import SOTransformer

so = Step(
    name='so',
    transformations=[
        FileTransformation(
            src_path='input/so/so.json',
            dst_path=lambda _, f: f'intermediate/so/so.{stem(f)}',
            transformer=SOTransformer,
        ),
    ],
)
