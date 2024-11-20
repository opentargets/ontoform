from ontoform.format import extension
from ontoform.model import FileTransformation, Step
from ontoform.transformers.disease import DiseaseTransformer

disease = Step(
    name='disease',
    transformations=[
        FileTransformation(
            src_path='input/ontology-inputs/efo_otar_slim.json',
            dst_path=lambda _, f: f'output/disease/disease.{extension(f)}',
            transformer=DiseaseTransformer,
        ),
    ],
)
