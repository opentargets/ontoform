from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import IOBase
from multiprocessing import get_context
from typing import Protocol

from loguru import logger

from ontoform.format import Format
from ontoform.storage import get_storage


class Transformer(Protocol):
    def transform(self, src: IOBase, dst: IOBase, output_format: Format) -> None: ...


class FileTransformation:
    def __init__(
        self,
        src_path: str,
        dst_path: str | Callable[[str, Format], str],
        transformer: type[Transformer],
    ):
        self.src_path = src_path
        self.dst_path = dst_path
        self.transformer = transformer

    def prepare(self, work_dir: str, output_format: Format) -> None:
        if callable(self.dst_path):
            self.dst_path = self.dst_path(self.src_path, output_format)

        self.src_path = f'{work_dir}/{self.src_path}'
        self.dst_path = f'{work_dir}/{self.dst_path}'
        self.output_format = output_format

        logger.debug(f'prepared transformation from {self.src_path} to {self.dst_path}')

    def execute(self) -> None:
        ss = get_storage(self.src_path)
        ds = get_storage(self.dst_path)
        ds.mkdir(ds.parent(self.dst_path))

        with ss.read(self.src_path) as src, ds.write(self.dst_path) as dst:
            self.transformer().transform(src, dst, self.output_format)

        logger.success(f'transformed {self.src_path} to {self.dst_path}')


class GlobTransformation:
    def __init__(
        self,
        src_prefix: str,
        dst_path: Callable[[str, str], str],
        glob: str = '*',
        transformer: type[Transformer] = Transformer,
    ):
        super().__init__()
        self.src_prefix = src_prefix
        self.dst_path = dst_path
        self.glob = glob
        self.transformer = transformer

    def explode(self, work_dir: str) -> list[FileTransformation]:
        src_path = f'{work_dir}/{self.src_prefix}'
        ss = get_storage(src_path)
        ts = []

        logger.debug(f'exploding transformations for {src_path}/{self.glob}')
        for p in ss.list(src_path, self.glob):
            p_without_workdir = p.replace(f'{work_dir}/', '')
            t = FileTransformation(p_without_workdir, self.dst_path, self.transformer)
            ts.append(t)

        return ts


class Step:
    def __init__(
        self,
        name: str,
        transformations: list[FileTransformation | GlobTransformation],
    ):
        self.name = name
        self.transformations = transformations

    def execute(
        self,
        work_dir: str,
        output_format: Format = None,
    ) -> None:
        logger.info(f'running step {self.name}')

        at = []
        for t in self.transformations:
            if isinstance(t, GlobTransformation):
                at += t.explode(work_dir)
            else:
                at.append(t)
        for t in at:
            t.prepare(work_dir, output_format)

        logger.debug(f'executing {len(at)} transformations')

        with ProcessPoolExecutor(max_workers=4, mp_context=get_context('spawn')) as executor:
            future_to_transform = {executor.submit(t.execute): t for t in at}

            for future in as_completed(future_to_transform):
                transform = future_to_transform[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f'Failed to execute transformation for {transform.src_path}: {e}')
