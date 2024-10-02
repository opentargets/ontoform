import os
from collections.abc import Callable
from importlib.metadata import version
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import typer
from loguru import logger

app = typer.Typer(
    add_completion=False,
    help='Transform ontologies into NDJSONs optimized for Open Targets pipeline',
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


def run_transformer(transformer: Callable[[Path, Path], None]) -> Callable[[Path, Path, typer.Context], None]:
    def run(src: Path, dst: Path, ctx: typer.Context) -> None:
        logger.info(f'running transformer {ctx.command.name}')
        logger.debug(f'source file: {src}')
        logger.debug(f'destination file: {dst}')
        transformer(src, dst)

    return run


def main() -> None:
    logger.info(f'starting ontoform v{version("ontoform")}')
    transformers = import_transformers()

    for name, transform in transformers.items():
        app.command(name=name)(run_transformer(transform))

    app()


if __name__ == '__main__':
    main()
