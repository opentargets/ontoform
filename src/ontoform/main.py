import os
from collections.abc import Callable
from importlib.metadata import version
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Annotated, BinaryIO

import typer
from loguru import logger

from ontoform.storage import get_storage
from ontoform.util import SupportedFormats

app = typer.Typer(
    add_completion=False,
    help='Preprocess input data for the Open Targets pipeline',
)


def import_transformers() -> dict[str, Callable[[Path, Path], None]]:
    transformers = {}
    transformer_dir = Path(os.path.abspath(__file__)).parent / 'transformers'

    for transformer_path in transformer_dir.glob('[!__]*.py'):
        transformer_name = transformer_path.stem

        logger.debug(f'loading transformer: {transformer_name}')

        spec = spec_from_file_location(transformer_name, transformer_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        transformers[transformer_name] = module.transform

    logger.info(f'available transformers: {", ".join(list(transformers.keys()))}')
    return transformers


def run_transformer(
    transformer: Callable[[BinaryIO, BinaryIO], None],
) -> Callable[[Path, Path, typer.Context], None]:
    def run(
        src_path: str,
        dst_path: str,
        output_format: Annotated[
            SupportedFormats | None,
            typer.Option(
                '--format',
                '-f',
                help='Output format',
                is_eager=True,
            ),
        ] = SupportedFormats.PARQUET.value,
        ctx: typer.Context = None,
    ) -> None:
        logger.info(f'running transformer {ctx.command.name}')
        logger.debug(f'output format: {output_format.value}')
        logger.debug(f'source: {src_path}')
        logger.debug(f'destination: {dst_path}')

        s = get_storage(src_path)

        with s.read(src_path) as src, s.write(dst_path) as dst:
            transformer(src, dst, output_format)

    return run


def main():
    logger.info(f'starting ontoform v{version("ontoform")}')
    transformers = import_transformers()

    for name, transform in transformers.items():
        app.command(name=name)(run_transformer(transform))

    app()
