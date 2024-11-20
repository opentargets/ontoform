from ontoform.format import extension
from ontoform.model import FileTransformation, Step
from ontoform.transformers.expression import NormalTissueTransformer, TissueTransformer

expression = Step(
    name='expression',
    transformations=[
        FileTransformation(
            src_path='input/expression-inputs/normal_tissue.tsv.zip',
            dst_path='input/expression-inputs/normal_tissue.tsv.gz',
            transformer=NormalTissueTransformer,
        ),
        FileTransformation(
            src_path='input/expression-inputs/map_with_efos.json',
            dst_path=lambda _, f: f'input/expression-inputs/tissue-translation-map.{extension(f)}',
            transformer=TissueTransformer,
        ),
    ],
)
