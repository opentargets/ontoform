from ontoform.format import extension
from ontoform.model import FileTransformation, Step
from ontoform.transformers.so import SOTransformer

so = Step(
    name='so',
    transformations=[
        FileTransformation(
            src_path='input/so-inputs/so.json',
            dst_path=lambda _, f: f'input/so-inputs/so.{extension(f)}',
            transformer=SOTransformer,
        ),
    ],
)
