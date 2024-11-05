import json
import subprocess
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import polars as pl


def transform(src: BinaryIO, dst: BinaryIO) -> None:
    jq_command = '.genes | {"genes": map({id: .id, name: .name})}'

    # Generate a temporary file path tp store the source file
    temp_path = Path(f'/tmp/{uuid4()}.json')

    temp_path.write_bytes(src.read())

    # Ejecuta jq con subprocess
    result = subprocess.run(
        ['jq', jq_command, temp_path],
        capture_output=True,
        text=True
    )

    # Verifica si el comando se ejecutó correctamente
    if result.returncode == 0:
        output = json.loads(result.stdout)
        input_genes = pl.DataFrame(output, strict=False, infer_schema_length=3)
        # extract genes list
        genes_list = input_genes.unnest('genes')
        # # read id and name
        output = genes_list.select(['id', 'name'])
        output.write_csv(dst, separator='\t', include_header=True)
