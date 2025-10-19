"""
Local filesystem storage adapter implementation.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import logging
import shutil
import os
import hashlib
import mimetypes

from ..base import (
    BaseStorageAdapter,
    StorageObject,
    PresignedUrl,
    StorageConnectionError,
    StorageOperationError,
    StorageNotFoundError,
    StoragePermissionError
)

logger = logging.getLogger(__name__)


class LocalStorageAdapter(BaseStorageAdapter):
    """
    Local filesystem storage adapter.
    
    Configuration required:
        - base_path: Base directory path for file storage
        - create_dirs: Whether to create directories automatically (default: True)
    
    Credentials: None required for local filesystem.
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize local storage adapter."""
        super().__init__(config, credentials)
        self._base_path = None
        self._initialize_storage()

    def _validate_config(self) -> None:
        """Validate local storage configuration."""
        if 'base_path' not in self.config:
            raise ValueError("Local adapter requires 'base_path' in config")
        
        if not self.config['base_path']:
            raise ValueError("base_path cannot be empty")

    def _initialize_storage(self) -> None:
        """Initialize local storage directory."""
        try:
            base_path = self.config['base_path']
            self._base_path = Path(base_path).resolve()
            
            # Create base directory if configured to do so
            create_dirs = self.config.get('create_dirs', True)
            
            if not self._base_path.exists():
                if create_dirs:
                    self._base_path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created base directory: {self._base_path}")
                else:
                    raise StorageConnectionError(
                        f"Base path does not exist: {self._base_path}"
                    )
            
            # Check if path is a directory
            if not self._base_path.is_dir():
                raise StorageConnectionError(
                    f"Base path is not a directory: {self._base_path}"
                )
            
            # Check write permissions
            if not os.access(self._base_path, os.W_OK):
                raise StoragePermissionError(
                    f"No write permission for base path: {self._base_path}"
                )
                
        except (OSError, PermissionError) as e:
            logger.error(f"Failed to initialize local storage: {e}")
            raise StorageConnectionError(f"Local storage initialization failed: {e}")

    def _get_full_path(self, key: str) -> Path:
        """Get full filesystem path for a key."""
        # Ensure key doesn't escape base directory
        key_path = Path(key)
        if key_path.is_absolute() or '..' in key_path.parts:
            raise ValueError(f"Invalid key: {key}")
        
        return self._base_path / key_path

    def test_connection(self, timeout: int = 10) -> bool:
        """Test local storage by checking directory access."""
        try:
            # Try to create a test file
            test_file = self._base_path / '.storage_test'
            test_file.write_text('test')
            test_file.unlink()
            
            logger.info(f"Successfully verified local storage at: {self._base_path}")
            return True
            
        except (OSError, PermissionError) as e:
            raise StorageConnectionError(f"Local storage test failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to local storage."""
        try:
            full_path = self._get_full_path(key)
            
            # Create parent directories if needed
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(full_path, 'wb') as f:
                shutil.copyfileobj(file_obj, f)
            
            # Store metadata in a sidecar file if provided
            if metadata or content_type:
                meta_path = full_path.with_suffix(full_path.suffix + '.meta')
                meta_data = metadata or {}
                if content_type:
                    meta_data['content_type'] = content_type
                
                import json
                with open(meta_path, 'w') as f:
                    json.dump(meta_data, f)
            
            logger.info(f"Uploaded file to local storage: {full_path}")
            return key
            
        except (OSError, PermissionError) as e:
            raise StorageOperationError(f"Local upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from local storage."""
        try:
            full_path = self._get_full_path(key)
            
            if not full_path.exists():
                raise StorageNotFoundError(f"File not found: {key}")
            
            if destination:
                destination = Path(destination)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(full_path, destination)
                logger.info(f"Downloaded file to: {destination}")
                with open(destination, 'rb') as f:
                    return f.read()
            else:
                with open(full_path, 'rb') as f:
                    data = f.read()
                logger.info(f"Downloaded file: {key}")
                return data
                
        except FileNotFoundError:
            raise StorageNotFoundError(f"File not found: {key}")
        except (OSError, PermissionError) as e:
            raise StorageOperationError(f"Local download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from local storage."""
        try:
            full_path = self._get_full_path(key)
            
            if not full_path.exists():
                raise StorageNotFoundError(f"File not found: {key}")
            
            full_path.unlink()
            
            # Also delete metadata file if exists
            meta_path = full_path.with_suffix(full_path.suffix + '.meta')
            if meta_path.exists():
                meta_path.unlink()
            
            logger.info(f"Deleted file: {key}")
            return True
            
        except FileNotFoundError:
            raise StorageNotFoundError(f"File not found: {key}")
        except (OSError, PermissionError) as e:
            raise StorageOperationError(f"Local delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in local storage."""
        try:
            full_path = self._get_full_path(key)
            return full_path.exists() and full_path.is_file()
        except Exception as e:
            raise StorageOperationError(f"Local file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from local storage."""
        try:
            full_path = self._get_full_path(key)
            
            if not full_path.exists():
                raise StorageNotFoundError(f"File not found: {key}")
            
            stat = full_path.stat()
            
            # Calculate ETag as MD5 hash
            md5_hash = hashlib.md5()
            with open(full_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    md5_hash.update(chunk)
            etag = md5_hash.hexdigest()
            
            # Try to read metadata file
            metadata = {}
            content_type = None
            meta_path = full_path.with_suffix(full_path.suffix + '.meta')
            
            if meta_path.exists():
                import json
                with open(meta_path, 'r') as f:
                    meta_data = json.load(f)
                    content_type = meta_data.pop('content_type', None)
                    metadata = meta_data
            
            # Guess content type if not stored
            if not content_type:
                content_type, _ = mimetypes.guess_type(str(full_path))
            
            return StorageObject(
                key=key,
                size=stat.st_size,
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                etag=etag,
                content_type=content_type,
                metadata=metadata if metadata else None
            )
            
        except FileNotFoundError:
            raise StorageNotFoundError(f"File not found: {key}")
        except Exception as e:
            raise StorageOperationError(f"Local metadata retrieval failed: {e}")

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files in local storage."""
        try:
            files = []
            search_path = self._base_path / prefix if prefix else self._base_path
            
            # Use rglob to recursively find files
            pattern = '**/*' if search_path.is_dir() else '*'
            
            for file_path in search_path.rglob('*') if search_path.is_dir() else self._base_path.rglob(f"{prefix}*"):
                # Skip directories and metadata files
                if file_path.is_dir() or file_path.suffix == '.meta':
                    continue
                
                # Get relative path as key
                try:
                    key = str(file_path.relative_to(self._base_path))
                    
                    # Skip if doesn't match prefix
                    if prefix and not key.startswith(prefix):
                        continue
                    
                    stat = file_path.stat()
                    
                    files.append(StorageObject(
                        key=key,
                        size=stat.st_size,
                        last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                        etag=None,  # Skip ETag calculation for listing performance
                        content_type=mimetypes.guess_type(str(file_path))[0]
                    ))
                    
                    if len(files) >= max_results:
                        break
                        
                except ValueError:
                    # Skip files outside base path
                    continue
            
            logger.info(f"Listed {len(files)} files from local storage with prefix: {prefix}")
            return files
            
        except Exception as e:
            raise StorageOperationError(f"Local list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """
        Generate presigned URL for local storage.
        
        Note: Local storage doesn't support true presigned URLs.
        This returns a file:// URL for local access only.
        """
        try:
            full_path = self._get_full_path(key)
            
            if not full_path.exists():
                raise StorageNotFoundError(f"File not found: {key}")
            
            # Return file:// URL (only works locally)
            file_url = full_path.as_uri()
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expiration)
            
            logger.warning(
                "Local storage presigned URLs are file:// URLs and only work locally"
            )
            
            return PresignedUrl(
                url=file_url,
                expires_at=expires_at,
                method=method.upper()
            )
            
        except FileNotFoundError:
            raise StorageNotFoundError(f"File not found: {key}")

    def get_public_url(self, key: str) -> Optional[str]:
        """
        Get public URL for local storage file.
        
        Note: Local storage doesn't support public URLs.
        Returns file:// URL for reference only.
        """
        full_path = self._get_full_path(key)
        return full_path.as_uri() if full_path.exists() else None