"""Storage adapter implementations."""

from .s3 import S3StorageAdapter
from .azure import AzureStorageAdapter
from .minio import MinIOStorageAdapter
from .local import LocalStorageAdapter

__all__ = [
    'S3StorageAdapter',
    'AzureStorageAdapter',
    'MinIOStorageAdapter',
    'LocalStorageAdapter',
]