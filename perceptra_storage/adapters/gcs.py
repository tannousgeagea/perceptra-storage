"""
Google Cloud Storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import json
import logging

try:
    from google.cloud import storage as gcs
    from google.api_core import exceptions as gcs_exceptions
except ImportError:
    raise ImportError(
        "google-cloud-storage is required for GCS adapter. "
        "Install with: pip install perceptra-storage[gcs]"
    )

from ..base import (
    BaseStorageAdapter,
    StorageObject,
    StoragePage,
    PresignedUrl,
    StorageConnectionError,
    StorageOperationError,
    StorageNotFoundError,
    StoragePermissionError
)

logger = logging.getLogger(__name__)

# V4 signed URLs are valid for at most 7 days.
MAX_SIGNED_URL_SECONDS = 7 * 24 * 3600


class GCSStorageAdapter(BaseStorageAdapter):
    """
    Google Cloud Storage adapter.

    Configuration required:
        - bucket_name: GCS bucket name
        - project_id: GCP project (optional)

    Credentials:
        - service_account_info: service-account key JSON (dict or JSON string)
        OR none: Application Default Credentials (workload identity, GCE).
          Signed URLs need a service-account key (can_presign is False otherwise).
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize GCS storage adapter."""
        super().__init__(config, credentials)
        self._client = None
        self._bucket = None
        self._service_account = None
        self._initialize_client()

    def _validate_config(self) -> None:
        """Validate GCS configuration."""
        if not self.config.get('bucket_name'):
            raise ValueError("GCS adapter requires 'bucket_name' in config")

    def _initialize_client(self) -> None:
        """Initialize the google-cloud-storage client."""
        try:
            info = (self.credentials or {}).get('service_account_info')
            if isinstance(info, str):
                info = json.loads(info)
            project = self.config.get('project_id') or (info or {}).get('project_id')
            if info:
                from google.oauth2 import service_account

                self._service_account = service_account.Credentials.from_service_account_info(info)
                self._client = gcs.Client(project=project, credentials=self._service_account)
            else:
                self._client = gcs.Client(project=project)
            self._bucket = self._client.bucket(self.config['bucket_name'])
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {type(e).__name__}")
            raise StorageConnectionError(f"GCS client initialization failed: {e}")

    @property
    def can_presign(self) -> bool:
        return self._service_account is not None

    def test_connection(self, timeout: int = 10) -> bool:
        """Test GCS connection by listing at most one object (works with read-only roles)."""
        try:
            next(iter(self._client.list_blobs(self._bucket, max_results=1, timeout=timeout)), None)
            logger.info(f"Successfully connected to GCS bucket: {self.config['bucket_name']}")
            return True
        except gcs_exceptions.NotFound:
            raise StorageConnectionError(f"GCS bucket '{self.config['bucket_name']}' does not exist")
        except gcs_exceptions.Forbidden as e:
            raise StoragePermissionError(f"GCS access denied: {e}")
        except Exception as e:
            raise StorageConnectionError(f"GCS connection failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to GCS."""
        try:
            blob = self._bucket.blob(key)
            if metadata:
                blob.metadata = metadata
            blob.upload_from_file(file_obj, content_type=content_type, rewind=True)
            logger.info(f"Uploaded file to GCS: {self.config['bucket_name']}/{key}")
            return key
        except gcs_exceptions.Forbidden:
            raise StoragePermissionError(f"Permission denied uploading to GCS: {key}")
        except Exception as e:
            raise StorageOperationError(f"GCS upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from GCS."""
        try:
            data = self._bucket.blob(key).download_as_bytes()
            if destination:
                Path(destination).write_bytes(data)
            return data
        except gcs_exceptions.NotFound:
            raise StorageNotFoundError(f"File not found in GCS: {key}")
        except gcs_exceptions.Forbidden:
            raise StoragePermissionError(f"Permission denied downloading from GCS: {key}")
        except Exception as e:
            raise StorageOperationError(f"GCS download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from GCS."""
        try:
            self._bucket.blob(key).delete()
            logger.info(f"Deleted GCS file: {key}")
            return True
        except gcs_exceptions.NotFound:
            raise StorageNotFoundError(f"File not found in GCS: {key}")
        except gcs_exceptions.Forbidden:
            raise StoragePermissionError(f"Permission denied deleting from GCS: {key}")
        except Exception as e:
            raise StorageOperationError(f"GCS delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in GCS."""
        try:
            return self._bucket.blob(key).exists()
        except Exception as e:
            raise StorageOperationError(f"GCS file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from GCS."""
        try:
            blob = self._bucket.get_blob(key)
        except Exception as e:
            raise StorageOperationError(f"GCS metadata retrieval failed: {e}")
        if blob is None:
            raise StorageNotFoundError(f"File not found in GCS: {key}")
        return StorageObject(
            key=key,
            size=blob.size or 0,
            last_modified=blob.updated,
            etag=blob.etag,
            content_type=blob.content_type,
            metadata=blob.metadata,
        )

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files in GCS bucket."""
        try:
            return [
                StorageObject(
                    key=blob.name,
                    size=blob.size or 0,
                    last_modified=blob.updated,
                    etag=blob.etag,
                    content_type=blob.content_type,
                )
                for blob in self._client.list_blobs(self._bucket, prefix=prefix or None, max_results=max_results)
            ]
        except Exception as e:
            raise StorageOperationError(f"GCS list operation failed: {e}")

    def download_range(self, key: str, offset: int = 0, length: int = 65536) -> bytes:
        """Ranged read (needs only storage.objects.get)."""
        try:
            return self._bucket.blob(key).download_as_bytes(start=offset, end=offset + length - 1)
        except gcs_exceptions.NotFound:
            raise StorageNotFoundError(f"File not found in GCS: {key}")
        except gcs_exceptions.RequestRangeNotSatisfiable:
            return b""
        except gcs_exceptions.Forbidden:
            raise StoragePermissionError(f"Permission denied reading from GCS: {key}")
        except Exception as e:
            raise StorageOperationError(f"GCS ranged download failed: {e}")

    def list_page(
        self,
        prefix: str = "",
        delimiter: str = "/",
        page_size: int = 200,
        continuation_token: Optional[str] = None,
    ) -> StoragePage:
        """Folder-style listing with GCS page tokens (needs storage.objects.list)."""
        try:
            iterator = self._client.list_blobs(
                self._bucket, prefix=prefix or None, delimiter=delimiter or None,
                max_results=page_size, page_token=continuation_token,
            )
            gcs_page = next(iterator.pages, None)
            page = StoragePage()
            if gcs_page is not None:
                page.objects = [
                    StorageObject(key=b.name, size=b.size or 0, last_modified=b.updated, etag=b.etag,
                                  content_type=b.content_type)
                    for b in gcs_page
                ]
                page.prefixes = sorted(gcs_page.prefixes)
            page.next_token = iterator.next_page_token
            return page
        except gcs_exceptions.Forbidden as e:
            raise StoragePermissionError(f"Listing bucket '{self.config['bucket_name']}' is not permitted: {e}")
        except Exception as e:
            raise StorageOperationError(f"GCS list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """Generate a V4 signed URL for a GCS object."""
        if method.upper() not in ('GET', 'PUT', 'DELETE'):
            raise StorageOperationError(f"Unsupported HTTP method: {method}")
        if not self.can_presign:
            raise StorageOperationError(
                "GCS signed URLs need service-account key credentials (service_account_info)"
            )
        expiration = min(int(expiration), MAX_SIGNED_URL_SECONDS)
        try:
            url = self._bucket.blob(key).generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration),
                method=method.upper(),
                credentials=self._service_account,
            )
        except Exception as e:
            raise StorageOperationError(f"GCS signed URL generation failed: {e}")
        return PresignedUrl(
            url=url,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=expiration),
            method=method.upper(),
        )

    def get_public_url(self, key: str) -> Optional[str]:
        """Public URL of a GCS object (if the bucket is public)."""
        return f"https://storage.googleapis.com/{self.config['bucket_name']}/{key}"
