"""
MinIO / S3-compatible storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import logging
from urllib.parse import urlparse

try:
    from minio import Minio
    from minio.error import S3Error, InvalidResponseError
    from urllib3.exceptions import MaxRetryError
except ImportError:
    raise ImportError(
        "minio is required for MinIO adapter. Install with: pip install minio"
    )

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


class MinIOStorageAdapter(BaseStorageAdapter):
    """
    MinIO / S3-compatible storage adapter.
    
    Configuration required:
        - bucket_name: Bucket name
        - endpoint_url: MinIO endpoint (e.g., 'play.min.io:9000')
        - secure: Whether to use HTTPS (default: True)
        - region: Region name (optional)
    
    Credentials required:
        - access_key: Access key ID
        - secret_key: Secret access key
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize MinIO storage adapter."""
        super().__init__(config, credentials)
        self._client = None
        self._initialize_client()

    def _validate_config(self) -> None:
        """Validate MinIO configuration."""
        required_fields = ['bucket_name', 'endpoint_url']
        missing = [f for f in required_fields if f not in self.config]
        
        if missing:
            raise ValueError(f"MinIO adapter requires: {', '.join(missing)}")
        
        if not self.config['bucket_name'] or not self.config['endpoint_url']:
            raise ValueError("bucket_name and endpoint_url cannot be empty")

    def _initialize_client(self) -> None:
        """Initialize MinIO client."""
        try:
            endpoint = self.config['endpoint_url']
            # Remove protocol if present
            if '://' in endpoint:
                parsed = urlparse(endpoint)
                endpoint = parsed.netloc or parsed.path
            
            secure = self.config.get('secure', True)
            region = self.config.get('region')
            
            # Build client kwargs
            client_kwargs = {
                'endpoint': endpoint,
                'secure': secure
            }
            
            if self.credentials:
                client_kwargs['access_key'] = self.credentials.get('access_key', '')
                client_kwargs['secret_key'] = self.credentials.get('secret_key', '')
            
            if region:
                client_kwargs['region'] = region
            
            self._client = Minio(**client_kwargs)
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise StorageConnectionError(f"MinIO client initialization failed: {e}")

    def test_connection(self, timeout: int = 10) -> bool:
        """Test MinIO connection by checking bucket access."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Check if bucket exists
            if not self._client.bucket_exists(bucket_name):
                raise StorageConnectionError(f"MinIO bucket '{bucket_name}' does not exist")
            
            logger.info(f"Successfully connected to MinIO bucket: {bucket_name}")
            return True
            
        except S3Error as e:
            if e.code == 'AccessDenied':
                raise StoragePermissionError(f"Access denied to MinIO bucket '{bucket_name}'")
            raise StorageConnectionError(f"MinIO connection failed: {e}")
        except (MaxRetryError, InvalidResponseError) as e:
            raise StorageConnectionError(f"MinIO connection failed: {e}")
        except Exception as e:
            raise StorageConnectionError(f"MinIO connection failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to MinIO."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Get file size
            file_obj.seek(0, 2)  # Seek to end
            file_size = file_obj.tell()
            file_obj.seek(0)  # Seek back to start
            
            self._client.put_object(
                bucket_name,
                key,
                file_obj,
                length=file_size,
                content_type=content_type or 'application/octet-stream',
                metadata=metadata
            )
            
            logger.info(f"Uploaded file to MinIO: {bucket_name}/{key}")
            return key
            
        except S3Error as e:
            if e.code == 'AccessDenied':
                raise StoragePermissionError(f"Permission denied uploading to MinIO: {key}")
            raise StorageOperationError(f"MinIO upload failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"MinIO upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from MinIO."""
        try:
            bucket_name = self.config['bucket_name']
            
            if destination:
                self._client.fget_object(bucket_name, key, str(destination))
                logger.info(f"Downloaded MinIO file to: {destination}")
                with open(destination, 'rb') as f:
                    return f.read()
            else:
                response = self._client.get_object(bucket_name, key)
                data = response.read()
                response.close()
                response.release_conn()
                logger.info(f"Downloaded MinIO file: {key}")
                return data
                
        except S3Error as e:
            if e.code == 'NoSuchKey':
                raise StorageNotFoundError(f"File not found in MinIO: {key}")
            elif e.code == 'AccessDenied':
                raise StoragePermissionError(f"Permission denied downloading from MinIO: {key}")
            raise StorageOperationError(f"MinIO download failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"MinIO download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from MinIO."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Check if file exists first
            if not self.file_exists(key):
                raise StorageNotFoundError(f"File not found in MinIO: {key}")
            
            self._client.remove_object(bucket_name, key)
            logger.info(f"Deleted MinIO file: {key}")
            return True
            
        except StorageNotFoundError:
            raise
        except S3Error as e:
            if e.code == 'AccessDenied':
                raise StoragePermissionError(f"Permission denied deleting from MinIO: {key}")
            raise StorageOperationError(f"MinIO delete failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"MinIO delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in MinIO."""
        try:
            bucket_name = self.config['bucket_name']
            self._client.stat_object(bucket_name, key)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            raise StorageOperationError(f"MinIO file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from MinIO."""
        try:
            bucket_name = self.config['bucket_name']
            stat = self._client.stat_object(bucket_name, key)
            
            return StorageObject(
                key=key,
                size=stat.size,
                last_modified=stat.last_modified,
                etag=stat.etag.strip('"') if stat.etag else None,
                content_type=stat.content_type,
                metadata=stat.metadata
            )
            
        except S3Error as e:
            if e.code == 'NoSuchKey':
                raise StorageNotFoundError(f"File not found in MinIO: {key}")
            raise StorageOperationError(f"MinIO metadata retrieval failed: {e}")

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files in MinIO bucket."""
        try:
            bucket_name = self.config['bucket_name']
            files = []
            
            objects = self._client.list_objects(
                bucket_name,
                prefix=prefix if prefix else None,
                recursive=True
            )
            
            for obj in objects:
                files.append(StorageObject(
                    key=obj.object_name,
                    size=obj.size,
                    last_modified=obj.last_modified,
                    etag=obj.etag.strip('"') if obj.etag else None
                ))
                
                if len(files) >= max_results:
                    break
            
            logger.info(f"Listed {len(files)} files from MinIO with prefix: {prefix}")
            return files
            
        except S3Error as e:
            raise StorageOperationError(f"MinIO list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """Generate presigned URL for MinIO object."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Map method to MinIO method
            if method.upper() == 'GET':
                url = self._client.presigned_get_object(
                    bucket_name,
                    key,
                    expires=timedelta(seconds=expiration)
                )
            elif method.upper() == 'PUT':
                url = self._client.presigned_put_object(
                    bucket_name,
                    key,
                    expires=timedelta(seconds=expiration)
                )
            else:
                raise ValueError(f"Unsupported HTTP method for MinIO presigned URL: {method}")
            
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expiration)
            
            return PresignedUrl(
                url=url,
                expires_at=expires_at,
                method=method.upper()
            )
            
        except S3Error as e:
            raise StorageOperationError(f"MinIO presigned URL generation failed: {e}")

    def get_public_url(self, key: str) -> Optional[str]:
        """Get public URL for MinIO object."""
        endpoint = self.config['endpoint_url']
        if '://' not in endpoint:
            protocol = 'https' if self.config.get('secure', True) else 'http'
            endpoint = f"{protocol}://{endpoint}"
        
        bucket_name = self.config['bucket_name']
        return f"{endpoint}/{bucket_name}/{key}"