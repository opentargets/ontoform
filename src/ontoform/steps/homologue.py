from ontoform.format import extension
from ontoform.model import GlobTransformation, Step
from ontoform.transformers.homologue import HomologueTransformer

homologue = Step(
    name='homologue',
    transformations=[
        GlobTransformation(
            src_prefix='input/target-inputs/homologue/gene-dictionary',
            dst_path=lambda p,
            f: f'input/target-inputs/homologue/gene-dictionary-transformed/{p.rsplit("/")[-1]}'.replace(
                '.json', f'.{extension(f)}'
            ),
            glob='**/*.json',
            transformer=HomologueTransformer,
        ),
    ],
)
