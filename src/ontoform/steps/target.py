from ontoform.file_format import stem
from ontoform.model import FileTransformation, Step
from ontoform.transformers.target import (
    EnsemblTransformer,
    EssentialityMatricesTransformer,
    GeneIdentifiersTransformer,
    GnomadTransformer,
    SubcellularLocationSSLTransformer,
    SubcellularLocationTransformer,
)

target = Step(
    name='expression',
    transformations=[
        FileTransformation(
            src_path='input/target-inputs/hpa/subcellular_location.tsv.zip',
            dst_path='input/target-inputs/hpa/subcellular_location.tsv.gz',
            transformer=SubcellularLocationTransformer,
        ),
        FileTransformation(
            src_path='input/target-inputs/hpa/subcellular_locations_ssl.tsv',
            dst_path=lambda _, f: f'input/target-inputs/hpa/subcellular_locations_ssl.{stem(f)}',
            transformer=SubcellularLocationSSLTransformer,
        ),
        FileTransformation(
            src_path='input/target-inputs/project-scores/essentiality_matrices.zip',
            dst_path=lambda _, f: f'input/target-inputs/project-scores/04_binaryDepScores.{stem(f)}',
            transformer=EssentialityMatricesTransformer,
        ),
        FileTransformation(
            src_path='input/target-inputs/project-scores/gene_identifiers_latest.csv.gz',
            dst_path=lambda _, f: f'input/target-inputs/project-scores/gene_identifiers_latest.{stem(f)}',
            transformer=GeneIdentifiersTransformer,
        ),
        FileTransformation(
            src_path='input/target-inputs/ensembl/homo_sapiens.json',
            dst_path=lambda _, f: f'input/target-inputs/ensembl/homo_sapiens.{stem(f)}',
            transformer=EnsemblTransformer,
        ),
        FileTransformation(
            src_path='input/target-inputs/gnomad/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz',
            dst_path='input/target-inputs/gnomad/gnomad_lof_by_gene.txt.gz',
            transformer=GnomadTransformer,
        ),
    ],
)
