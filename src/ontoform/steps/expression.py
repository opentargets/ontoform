from ontoform.file_format import stem
from ontoform.model import FileTransformation, Step
from ontoform.transformers.expression import NormalTissueTransformer, TissueTransformer

expression = Step(
    name='expression',
    transformations=[
        FileTransformation(
            src_path='input/expression/normal_tissue.tsv.zip',
            dst_path='intermediate/expression/normal_tissue.tsv.gz',
            transformer=NormalTissueTransformer,
        ),
        FileTransformation(
            src_path='input/expression/map_with_efos.json',
            dst_path=lambda _, f: f'intermediate/expression/tissue-translation-map.{stem(f)}',
            transformer=TissueTransformer,
        ),
    ],
)
