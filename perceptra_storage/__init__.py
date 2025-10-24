"""
Perceptra Storage - Unified storage adapter interface for multi-cloud storage.

This package provides a consistent API for interacting with different storage backends:
- Amazon S3
- Azure Blob Storage
- MinIO / S3-compatible storage
- Local filesystem

Usage:
    >>> from perceptra_storage import get_storage_adapter
    >>> 
    >>> config = {'bucket_name': 'my-bucket', 'region': 'us-west-2'}
    >>> credentials = {'access_key_id': 'xxx', 'secret_access_key': 'yyy'}
    >>> 
    >>> adapter = get_storage_adapter('s3', config, credentials)
    >>> adapter.test_connection()
    True
    >>> 
    >>> # Upload a file
    >>> with open('data.csv', 'rb') as f:
    >>>     adapter.upload_file(f, 'datasets/data.csv')
    >>> 
    >>> # Download a file
    >>> data = adapter.download_file('datasets/data.csv')
"""

__version__ = '0.1.1'
__author__ = 'Perceptra Team'
__license__ = 'MIT'

# Import main components
from .base import (
    BaseStorageAdapter,
    StorageObject,
    PresignedUrl,
    StorageError,
    StorageConnectionError,
    StorageOperationError,
    StorageNotFoundError,
    StoragePermissionError,
)

from .factory import (
    get_storage_adapter,
    register_adapter,
    list_available_backends,
    get_adapter_info,
    StorageAdapterError,
)

# Import adapters for direct access
from .adapters.s3 import S3StorageAdapter
from .adapters.azure import AzureStorageAdapter
from .adapters.minio import MinIOStorageAdapter
from .adapters.local import LocalStorageAdapter

__all__ = [
    # Version
    '__version__',
    
    # Base classes and types
    'BaseStorageAdapter',
    'StorageObject',
    'PresignedUrl',
    
    # Exceptions
    'StorageError',
    'StorageConnectionError',
    'StorageOperationError',
    'StorageNotFoundError',
    'StoragePermissionError',
    'StorageAdapterError',
    
    # Factory functions
    'get_storage_adapter',
    'register_adapter',
    'list_available_backends',
    'get_adapter_info',
    
    # Adapters
    'S3StorageAdapter',
    'AzureStorageAdapter',
    'MinIOStorageAdapter',
    'LocalStorageAdapter',
]