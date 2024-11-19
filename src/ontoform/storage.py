import os
import sys
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from fnmatch import fnmatch
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

from google.cloud import storage
from loguru import logger


class Storage(ABC):
    """Abstract class for storage backends."""

    @abstractmethod
    def read(self, path: str) -> Generator[BinaryIO, None, None]:
        pass

    @abstractmethod
    def write(self, path: str) -> Generator[BinaryIO, None, None]:
        pass

    @abstractmethod
    def list(self, path: str, glob: str = '*') -> list[str]:
        pass

    @abstractmethod
    def mkdir(self, path: str) -> None:
        pass

    @abstractmethod
    def parent(self, path: str) -> str:
        pass


class LocalStorage(Storage):
    """Local storage backend."""

    @contextmanager
    def read(self, path: str) -> Generator[BinaryIO, None, None]:
        p = Path(path)
        if not p.exists():
            logger.critical(f'file not found {path}')
            sys.exit(1)

        try:
            with open(p, 'rb') as f:
                yield f
        except OSError as e:
            logger.critical(f'failed opening file {path}')
            logger.error(e)
            sys.exit(1)

    @contextmanager
    def write(self, path: str) -> Generator[BinaryIO, None, None]:
        with open(Path(path), 'wb') as f:
            yield f

    def list(self, path: str, glob: str = '*') -> list[str]:
        p = Path(path)
        return [str(f) for f in p.glob(glob) if f.is_file()]

    def mkdir(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    def parent(self, path: str) -> str:
        return Path(path).parent


class GoogleStorage(Storage):
    """Google Cloud Storage backend."""

    def __init__(self, url: str):
        self.client = storage.Client()
        self.bucket_name = url.replace('gs://', '').split('/', 1)[0]
        self.bucket = self.client.bucket(self.bucket_name)

    def trim_path(self, path: str) -> str:
        return path.replace('gs://', '').replace(self.bucket_name, '').lstrip('/')

    @contextmanager
    def read(self, path: str) -> Generator[BinaryIO, None, None]:
        logger.debug(f'reading from {path}')
        blob = self.bucket.blob(self.trim_path(path))

        if not blob.exists():
            logger.critical(f'file not found {path}')
            sys.exit(1)

        try:
            with blob.open('rb') as f:
                yield f
        except OSError as e:
            logger.critical(f'failed opening file {path}')
            logger.error(e)
            sys.exit(1)

    @contextmanager
    def write(self, path: str) -> Generator[BinaryIO, None, None]:
        # we need to write to a tempfile first, polars crashes when writing directly to a gcs blob
        temp_path = os.getenv('TMPDIR', '.')
        temp_filename = Path(temp_path) / str(uuid4())

        try:
            with open(temp_filename, 'wb') as f:
                logger.debug(f'writing to tempfile {temp_filename}')
                yield f
        except OSError as e:
            logger.critical(f'failed writing tempfile {temp_filename}')
            logger.error(e)
            sys.exit(1)

        try:
            with open(temp_filename, 'rb') as f:
                logger.debug(f'writing finished, writing to final path {path}')
                blob = self.bucket.blob(self.trim_path(path))
                blob.upload_from_file(f)
        except OSError as e:
            logger.critical(f'failed writing file {path}')
            logger.error(e)

        os.unlink(temp_filename)

    def list(self, path: str, glob: str = '*') -> list[str]:
        prefix = self.trim_path(path)
        blobs = self.bucket.list_blobs(prefix=prefix)
        return [f'gs://{self.bucket.name}/{blob.name}' for blob in blobs if fnmatch(blob.name, glob)]

    def mkdir(self, path: str) -> None:
        pass

    def parent(self, path: str) -> str:
        return path.rsplit('/', 1)[0]


def get_storage(url: str) -> Storage:
    """Create a storage backend."""
    if url.startswith('gs://'):
        return GoogleStorage(url)
    return LocalStorage()


def normalize_path(path: str) -> str:
    if path.startswith('gs://'):
        return path
    return Path(path).absolute()
