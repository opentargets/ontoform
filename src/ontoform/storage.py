import sys
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

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
        logger.debug(f'writing to {path}')
        blob = self.bucket.blob(self.trim_path(path))

        try:
            with blob.open('wb', ignore_flush=True) as f:
                yield f
        except OSError as e:
            logger.critical(f'failed opening file {path}')
            logger.error(e)
            sys.exit(1)


def get_storage(url: str) -> Storage:
    """Create a storage backend."""
    if url.startswith('gs://'):
        return GoogleStorage(url)
    return LocalStorage()
