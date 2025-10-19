"""
Base storage adapter interface for perceptra-storage package.

This module defines the abstract base class that all storage adapters must implement.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, BinaryIO, Dict, Any
from pathlib import Path


class StorageError(Exception):
    """Base exception for storage operations."""
    pass


class StorageConnectionError(StorageError):
    """Raised when connection to storage backend fails."""
    pass


class StorageOperationError(StorageError):
    """Raised when a storage operation fails."""
    pass


class StorageNotFoundError(StorageError):
    """Raised when a requested file or object doesn't exist."""
    pass


class StoragePermissionError(StorageError):
    """Raised when operation is not permitted due to permissions."""
    pass


@dataclass
class StorageObject:
    """Represents metadata about a stored object."""
    key: str
    size: int
    last_modified: datetime
    etag: Optional[str] = None
    content_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PresignedUrl:
    """Represents a presigned URL for temporary access."""
    url: str
    expires_at: datetime
    method: str = "GET"


class BaseStorageAdapter(ABC):
    """
    Abstract base class for storage adapters.
    
    All concrete storage adapters (S3, Azure, MinIO, Local) must implement
    this interface to ensure consistent behavior across different backends.
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """
        Initialize storage adapter.

        Args:
            config: Backend-specific configuration (bucket name, region, etc.)
            credentials: Authentication credentials (access keys, tokens, etc.)
        
        Note:
            Credentials should never be logged or stored in plain text.
        """
        self.config = config
        self.credentials = credentials or {}
        self._validate_config()

    @abstractmethod
    def _validate_config(self) -> None:
        """
        Validate that required configuration parameters are present.
        
        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        pass

    @abstractmethod
    def test_connection(self, timeout: int = 10) -> bool:
        """
        Test connection to storage backend.

        Args:
            timeout: Maximum time in seconds to wait for connection.

        Returns:
            True if connection successful.

        Raises:
            StorageConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Upload a file to storage.

        Args:
            file_obj: File-like object to upload.
            key: Destination path/key in storage.
            content_type: MIME type of the file.
            metadata: Additional metadata to attach to the file.

        Returns:
            The key/path where the file was stored.

        Raises:
            StorageOperationError: If upload fails.
        """
        pass

    @abstractmethod
    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """
        Download a file from storage.

        Args:
            key: Path/key of the file to download.
            destination: Optional local path to save the file.

        Returns:
            File contents as bytes if destination is None.

        Raises:
            StorageNotFoundError: If file doesn't exist.
            StorageOperationError: If download fails.
        """
        pass

    @abstractmethod
    def delete_file(self, key: str) -> bool:
        """
        Delete a file from storage.

        Args:
            key: Path/key of the file to delete.

        Returns:
            True if deletion successful.

        Raises:
            StorageNotFoundError: If file doesn't exist.
            StorageOperationError: If deletion fails.
        """
        pass

    @abstractmethod
    def file_exists(self, key: str) -> bool:
        """
        Check if a file exists in storage.

        Args:
            key: Path/key of the file to check.

        Returns:
            True if file exists, False otherwise.
        """
        pass

    @abstractmethod
    def get_file_metadata(self, key: str) -> StorageObject:
        """
        Get metadata about a stored file.

        Args:
            key: Path/key of the file.

        Returns:
            StorageObject with file metadata.

        Raises:
            StorageNotFoundError: If file doesn't exist.
        """
        pass

    @abstractmethod
    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """
        List files in storage with optional prefix filter.

        Args:
            prefix: Filter results to keys starting with this prefix.
            max_results: Maximum number of results to return.

        Returns:
            List of StorageObject instances.

        Raises:
            StorageOperationError: If listing fails.
        """
        pass

    @abstractmethod
    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """
        Generate a presigned URL for temporary access.

        Args:
            key: Path/key of the file.
            expiration: URL expiration time in seconds.
            method: HTTP method (GET, PUT, etc.).

        Returns:
            PresignedUrl object with URL and expiration.

        Raises:
            StorageOperationError: If URL generation fails.
        """
        pass

    def get_public_url(self, key: str) -> Optional[str]:
        """
        Get public URL for a file (if supported by backend).

        Args:
            key: Path/key of the file.

        Returns:
            Public URL string or None if not publicly accessible.
        """
        return None

    def __repr__(self) -> str:
        """String representation without exposing credentials."""
        backend_type = self.__class__.__name__
        config_keys = list(self.config.keys())
        return f"<{backend_type} config_keys={config_keys}>"