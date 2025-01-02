from ontoform.file_format import stem
from ontoform.model import GlobTransformation, Step
from ontoform.transformers.homologue import HomologueTransformer

homologue = Step(
    name='homologue',
    transformations=[
        GlobTransformation(
            src_prefix='input/target-inputs/homologue/gene_dictionary',
            dst_path=lambda p, f: f'input/target-inputs/homologue/gene_dictionary/{p.rsplit("/")[-1]}'.replace(
                '.json', f'.{stem(f)}'
            ),
            glob='**/*.json',
            transformer=HomologueTransformer,
        ),
    ],
)
