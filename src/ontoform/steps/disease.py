from ontoform.file_format import stem
from ontoform.model import FileTransformation, Step
from ontoform.transformers.disease import DiseaseTransformer

disease = Step(
    name='disease',
    transformations=[
        FileTransformation(
            src_path='input/ontology-inputs/efo_otar_slim.json',
            dst_path=lambda _, f: f'output/disease/disease.{stem(f)}',
            transformer=DiseaseTransformer,
        ),
    ],
)
