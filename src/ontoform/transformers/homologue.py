import json
import subprocess
from pathlib import Path

import polars as pl


def transform(src: Path, dst: Path) -> None:
    jq_command = '.genes | {"genes": map({id: .id, name: .name})}'

    # Ejecuta jq con subprocess
    result = subprocess.run(
        ['jq', jq_command, src],
        capture_output=True,
        text=True
    )

    # Verifica si el comando se ejecutó correctamente
    if result.returncode == 0:
        # Carga la salida JSON como un objeto de Python
        output = json.loads(result.stdout)

        # Crea un DataFrame de Polars a partir de la salida JSON
        input_genes = pl.DataFrame(output, strict=False,infer_schema_length=3)

        # prepare node data

        # extract genes list
        genes_list = input_genes.unnest('genes')

        # # read id and name
        output = genes_list.select(['id', 'name'])

        # # write the result
        output.write_csv(dst, separator='\t', include_header=True)
