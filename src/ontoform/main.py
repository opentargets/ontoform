import os
from collections.abc import Callable
from importlib.metadata import version
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger

from ontoform.file_format import FileFormat
from ontoform.model import Step
from ontoform.storage import normalize_path


def load_steps() -> dict[str, Step]:
    steps = {}
    steps_dir = Path(os.path.abspath(__file__)).parent / 'steps'

    for step_path in steps_dir.glob('[!__]*.py'):
        step_name = step_path.stem
        logger.debug(f'loading step: {step_name}')
        spec = spec_from_file_location(step_name, step_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        steps[step_name] = getattr(module, step_name)

    logger.info(f'available steps: {", ".join(list(steps.keys()))}')
    return steps


def create_command(step: Step) -> Callable:
    wrapper: Callable[..., None]

    def wrapper(
        ctx: typer.Context,
        output_format: Annotated[
            FileFormat,
            typer.Option(
                '--output-format',
                '-o',
                help='Output format',
                is_eager=True,
            ),
        ] = FileFormat.PARQUET.value,
    ) -> None:
        work_dir = ctx.obj.get('work_dir')

        return step.execute(work_dir, FileFormat(output_format))

    wrapper.__name__ = step.execute.__name__
    wrapper.__doc__ = step.execute.__doc__
    return wrapper


def setup_app() -> typer.Typer:
    app = typer.Typer(
        add_completion=False,
        help='Preprocess input data for the Open Targets pipeline',
    )

    @app.callback()
    def main(
        ctx: typer.Context,
        work_dir: str = typer.Option(
            ...,
            '--work-dir',
            '-w',
            help='Working directory',
            callback=normalize_path,
        ),
    ) -> None:
        ctx.obj = {'work_dir': work_dir}

    steps = load_steps()
    for step_name, step in steps.items():
        wrapped_step = create_command(step)
        app.command(name=step_name)(wrapped_step)

    return app


def main():
    logger.info(f'starting ontoform v{version("ontoform")}')
    app = setup_app()
    app(obj={})
