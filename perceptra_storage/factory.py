"""
Factory for creating storage adapter instances.
"""
from typing import Dict, Any, Optional
import logging

from .base import BaseStorageAdapter
from .adapters.s3 import S3StorageAdapter
from .adapters.azure import AzureStorageAdapter
from .adapters.minio import MinIOStorageAdapter
from .adapters.local import LocalStorageAdapter

logger = logging.getLogger(__name__)


# Registry of available adapters
ADAPTER_REGISTRY = {
    's3': S3StorageAdapter,
    'azure': AzureStorageAdapter,
    'minio': MinIOStorageAdapter,
    'local': LocalStorageAdapter,
}

# GCS is optional: registered only when google-cloud-storage is installed.
try:
    from .adapters.gcs import GCSStorageAdapter
    ADAPTER_REGISTRY['gcs'] = GCSStorageAdapter
except ImportError:  # pragma: no cover - depends on the installed extras
    GCSStorageAdapter = None


class StorageAdapterError(Exception):
    """Raised when adapter creation fails."""
    pass


def get_storage_adapter(
    backend: str,
    config: Dict[str, Any],
    credentials: Optional[Dict[str, Any]] = None,
    read_only: bool = False,
) -> BaseStorageAdapter:
    """
    Factory function to create storage adapter instances.

    Args:
        backend: Backend type ('s3', 'azure', 'minio', 'gcs', 'local')
        config: Backend-specific configuration dictionary
        credentials: Optional credentials dictionary
        read_only: Wrap the adapter so uploads and deletes are refused
            (external storage that is only referenced, never owned)
    
    Returns:
        Initialized storage adapter instance
    
    Raises:
        StorageAdapterError: If backend is unknown or initialization fails
    
    Example:
        >>> config = {'bucket_name': 'my-bucket', 'region': 'us-west-2'}
        >>> credentials = {'access_key_id': 'xxx', 'secret_access_key': 'yyy'}
        >>> adapter = get_storage_adapter('s3', config, credentials)
        >>> adapter.test_connection()
        True
    """
    backend = backend.lower().strip()
    
    if backend not in ADAPTER_REGISTRY:
        available = ', '.join(ADAPTER_REGISTRY.keys())
        raise StorageAdapterError(
            f"Unknown storage backend: '{backend}'. "
            f"Available backends: {available}"
        )
    
    adapter_class = ADAPTER_REGISTRY[backend]
    
    try:
        logger.info(f"Creating {backend} storage adapter{' (read-only)' if read_only else ''}")
        adapter = adapter_class(config=config, credentials=credentials)
        if read_only:
            from .readonly import ReadOnlyStorageAdapter
            adapter = ReadOnlyStorageAdapter(adapter)
        return adapter
        
    except Exception as e:
        logger.error(f"Failed to create {backend} adapter: {e}")
        raise StorageAdapterError(
            f"Failed to initialize {backend} adapter: {e}"
        ) from e


def register_adapter(backend: str, adapter_class: type) -> None:
    """
    Register a custom storage adapter.
    
    Args:
        backend: Backend identifier (e.g., 'custom_s3')
        adapter_class: Adapter class that inherits from BaseStorageAdapter
    
    Raises:
        ValueError: If adapter_class doesn't inherit from BaseStorageAdapter
    
    Example:
        >>> class CustomAdapter(BaseStorageAdapter):
        ...     pass
        >>> register_adapter('custom', CustomAdapter)
    """
    if not issubclass(adapter_class, BaseStorageAdapter):
        raise ValueError(
            f"Adapter class must inherit from BaseStorageAdapter, "
            f"got {adapter_class.__name__}"
        )
    
    backend = backend.lower().strip()
    ADAPTER_REGISTRY[backend] = adapter_class
    logger.info(f"Registered custom adapter: {backend}")


def list_available_backends() -> list[str]:
    """
    Get list of available storage backends.
    
    Returns:
        List of backend identifiers
    
    Example:
        >>> list_available_backends()
        ['s3', 'azure', 'minio', 'local']
    """
    return list(ADAPTER_REGISTRY.keys())


def get_adapter_info(backend: str) -> Dict[str, Any]:
    """
    Get information about a specific adapter.
    
    Args:
        backend: Backend identifier
    
    Returns:
        Dictionary with adapter information
    
    Raises:
        StorageAdapterError: If backend is unknown
    """
    backend = backend.lower().strip()
    
    if backend not in ADAPTER_REGISTRY:
        raise StorageAdapterError(f"Unknown storage backend: '{backend}'")
    
    adapter_class = ADAPTER_REGISTRY[backend]
    
    return {
        'backend': backend,
        'class_name': adapter_class.__name__,
        'module': adapter_class.__module__,
        'docstring': adapter_class.__doc__,
    }