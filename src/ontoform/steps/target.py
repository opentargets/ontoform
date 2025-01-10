from ontoform.file_format import stem
from ontoform.model import FileTransformation, GlobTransformation, Step
from ontoform.transformers.target import (
    EnsemblTransformer,
    EssentialityMatricesTransformer,
    GeneIdentifiersTransformer,
    GnomadTransformer,
    HomologueTransformer,
    SubcellularLocationSSLTransformer,
    SubcellularLocationTransformer,
)

target = Step(
    name='expression',
    transformations=[
        GlobTransformation(
            src_prefix='input/target/homologue/gene_dictionary',
            dst_path=lambda p, f: f'intermediate/target/homologue/gene_dictionary/{p.rsplit("/")[-1]}'.replace(
                '.json', f'.{stem(f)}'
            ),
            glob='**/*.json',
            transformer=HomologueTransformer,
        ),
        FileTransformation(
            src_path='input/target/hpa/subcellular_location.tsv.zip',
            dst_path='intermediate/target/hpa/subcellular_location.tsv.gz',
            transformer=SubcellularLocationTransformer,
        ),
        FileTransformation(
            src_path='input/target/hpa/subcellular_locations_ssl.tsv',
            dst_path=lambda _, f: f'intermediate/target/hpa/subcellular_locations_ssl.{stem(f)}',
            transformer=SubcellularLocationSSLTransformer,
        ),
        FileTransformation(
            src_path='input/target/project-scores/essentiality_matrices.zip',
            dst_path=lambda _, f: f'intermediate/target/project-scores/04_binaryDepScores.{stem(f)}',
            transformer=EssentialityMatricesTransformer,
        ),
        FileTransformation(
            src_path='input/target/project-scores/gene_identifiers_latest.csv.gz',
            dst_path=lambda _, f: f'intermediate/target/project-scores/gene_identifiers_latest.{stem(f)}',
            transformer=GeneIdentifiersTransformer,
        ),
        FileTransformation(
            src_path='input/target/ensembl/homo_sapiens.json',
            dst_path=lambda _, f: f'intermediate/target/ensembl/homo_sapiens.{stem(f)}',
            transformer=EnsemblTransformer,
        ),
        FileTransformation(
            src_path='input/target/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz',
            dst_path='intermediate/target/gnomad/gnomad_lof_by_gene.txt.gz',
            transformer=GnomadTransformer,
        ),
    ],
)
