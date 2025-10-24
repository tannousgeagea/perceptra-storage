"""
Azure Blob Storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import logging

try:
    from azure.storage.blob import (
        BlobServiceClient,
        BlobClient,
        ContainerClient,
        generate_blob_sas,
        BlobSasPermissions
    )
    from azure.core.exceptions import (
        ResourceNotFoundError,
        ClientAuthenticationError,
        ServiceRequestError,
        HttpResponseError
    )
except ImportError:
    raise ImportError(
        "azure-storage-blob is required for Azure adapter. "
        "Install with: pip install azure-storage-blob"
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


class AzureStorageAdapter(BaseStorageAdapter):
    """
    Azure Blob Storage adapter.
    
    Configuration required:
        - container_name: Azure container name
        - account_name: Azure storage account name
    
    Credentials required:
        - account_key: Azure storage account key
        OR
        - sas_token: Shared Access Signature token
        OR
        - connection_string: Full connection string
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize Azure storage adapter."""
        super().__init__(config, credentials)
        self._blob_service_client = None
        self._container_client = None
        self._initialize_client()

    def _validate_config(self) -> None:
        """Validate Azure configuration."""
        required_fields = ['container_name', 'account_name']
        missing = [f for f in required_fields if f not in self.config]
        
        if missing:
            raise ValueError(f"Azure adapter requires: {', '.join(missing)}")
        
        if not self.config['container_name'] or not self.config['account_name']:
            raise ValueError("container_name and account_name cannot be empty")

    def _initialize_client(self) -> None:
        """Initialize Azure blob service client."""
        try:
            account_name = self.config['account_name']
            container_name = self.config['container_name']
            
            # Try connection string first
            if self.credentials and 'connection_string' in self.credentials:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    self.credentials['connection_string']
                )
            # Try account key
            elif self.credentials and 'account_key' in self.credentials:
                account_url = f"https://{account_name}.blob.core.windows.net"
                self._blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.credentials['account_key']
                )
            # Try SAS token
            elif self.credentials and 'sas_token' in self.credentials:
                account_url = f"https://{account_name}.blob.core.windows.net"
                sas_token = self.credentials['sas_token']
                if not sas_token.startswith('?'):
                    sas_token = f"?{sas_token}"
                self._blob_service_client = BlobServiceClient(
                    account_url=f"{account_url}{sas_token}"
                )
            else:
                raise ValueError(
                    "Azure adapter requires one of: connection_string, account_key, or sas_token"
                )
            
            self._container_client = self._blob_service_client.get_container_client(
                container_name
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize Azure client: {e}")
            raise StorageConnectionError(f"Azure client initialization failed: {e}")

    def test_connection(self, timeout: int = 10) -> bool:
        """Test Azure connection by checking container access."""
        try:
            container_name = self.config['container_name']
            
            # Check if container exists and is accessible
            properties = self._container_client.get_container_properties(timeout=timeout)
            logger.info(f"Successfully connected to Azure container: {container_name}")
            return True
            
        except ResourceNotFoundError:
            raise StorageConnectionError(
                f"Azure container '{self.config['container_name']}' does not exist"
            )
        except ClientAuthenticationError as e:
            raise StoragePermissionError(f"Azure authentication failed: {e}")
        except (ServiceRequestError, HttpResponseError) as e:
            raise StorageConnectionError(f"Azure connection failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to Azure Blob Storage."""
        try:
            blob_client = self._container_client.get_blob_client(key)
            
            # Prepare content settings
            content_settings = {}
            if content_type:
                from azure.storage.blob import ContentSettings
                content_settings = ContentSettings(content_type=content_type)
            
            blob_client.upload_blob(
                file_obj,
                overwrite=True,
                content_settings=content_settings if content_settings else None,
                metadata=metadata
            )
            
            logger.info(f"Uploaded file to Azure: {self.config['container_name']}/{key}")
            return key
            
        except HttpResponseError as e:
            if e.status_code == 403:
                raise StoragePermissionError(f"Permission denied uploading to Azure: {key}")
            raise StorageOperationError(f"Azure upload failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"Azure upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from Azure Blob Storage."""
        try:
            blob_client = self._container_client.get_blob_client(key)
            
            if destination:
                with open(destination, 'wb') as f:
                    download_stream = blob_client.download_blob()
                    download_stream.readinto(f)
                logger.info(f"Downloaded Azure file to: {destination}")
                with open(destination, 'rb') as f:
                    return f.read()
            else:
                download_stream = blob_client.download_blob()
                data = download_stream.readall()
                logger.info(f"Downloaded Azure file: {key}")
                return data
                
        except ResourceNotFoundError:
            raise StorageNotFoundError(f"File not found in Azure: {key}")
        except HttpResponseError as e:
            if e.status_code == 403:
                raise StoragePermissionError(f"Permission denied downloading from Azure: {key}")
            raise StorageOperationError(f"Azure download failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"Azure download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from Azure Blob Storage."""
        try:
            blob_client = self._container_client.get_blob_client(key)
            blob_client.delete_blob()
            logger.info(f"Deleted Azure file: {key}")
            return True
            
        except ResourceNotFoundError:
            raise StorageNotFoundError(f"File not found in Azure: {key}")
        except HttpResponseError as e:
            if e.status_code == 403:
                raise StoragePermissionError(f"Permission denied deleting from Azure: {key}")
            raise StorageOperationError(f"Azure delete failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"Azure delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in Azure Blob Storage."""
        try:
            blob_client = self._container_client.get_blob_client(key)
            return blob_client.exists()
        except Exception as e:
            raise StorageOperationError(f"Azure file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from Azure Blob Storage."""
        try:
            blob_client = self._container_client.get_blob_client(key)
            properties = blob_client.get_blob_properties()
            
            return StorageObject(
                key=key,
                size=properties.size,
                last_modified=properties.last_modified,
                etag=properties.etag.strip('"') if properties.etag else None,
                content_type=properties.content_settings.content_type
                    if properties.content_settings else None,
                metadata=properties.metadata
            )
            
        except ResourceNotFoundError:
            raise StorageNotFoundError(f"File not found in Azure: {key}")
        except Exception as e:
            raise StorageOperationError(f"Azure metadata retrieval failed: {e}")

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files in Azure container."""
        try:
            files = []
            blob_list = self._container_client.list_blobs(
                name_starts_with=prefix if prefix else None,
                results_per_page=max_results
            )
            
            for blob in blob_list:
                files.append(StorageObject(
                    key=blob.name,
                    size=blob.size,
                    last_modified=blob.last_modified,
                    etag=blob.etag.strip('"') if blob.etag else None,
                    content_type=blob.content_settings.content_type
                        if blob.content_settings else None
                ))
                
                if len(files) >= max_results:
                    break
            
            logger.info(f"Listed {len(files)} files from Azure with prefix: {prefix}")
            return files
            
        except Exception as e:
            raise StorageOperationError(f"Azure list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """Generate SAS URL for Azure blob."""
        try:
            # Map method to Azure permissions
            permission_map = {
                'GET': BlobSasPermissions(read=True),
                'PUT': BlobSasPermissions(write=True, create=True),
                'DELETE': BlobSasPermissions(delete=True)
            }
            
            permissions = permission_map.get(method.upper())
            if not permissions:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check if we have account key for SAS generation
            account_key = None
            
            if self.credentials and 'account_key' in self.credentials:
                account_key = self.credentials['account_key']
            elif self.credentials and 'connection_string' in self.credentials:
                # Parse connection string to extract account key
                conn_str = self.credentials['connection_string']
                conn_parts = dict(part.split('=', 1) for part in conn_str.split(';') if '=' in part)
                account_key = conn_parts.get('AccountKey')
            
            if not account_key:
                raise StorageOperationError(
                    "Account key or connection string with AccountKey required for SAS URL generation"
                )
            
            
            start_time = datetime.now(timezone.utc)
            expiry_time = start_time + timedelta(seconds=expiration)
            
            sas_token = generate_blob_sas(
                account_name=self.config['account_name'],
                container_name=self.config['container_name'],
                blob_name=key,
                account_key=self.credentials['account_key'],
                permission=permissions,
                expiry=expiry_time,
                start=start_time
            )
            
            blob_url = (
                f"https://{self.config['account_name']}.blob.core.windows.net/"
                f"{self.config['container_name']}/{key}?{sas_token}"
            )
            
            return PresignedUrl(
                url=blob_url,
                expires_at=expiry_time,
                method=method.upper()
            )
            
        except Exception as e:
            raise StorageOperationError(f"Azure SAS URL generation failed: {e}")

    def get_public_url(self, key: str) -> Optional[str]:
        """Get public URL for Azure blob (if container is public)."""
        return (
            f"https://{self.config['account_name']}.blob.core.windows.net/"
            f"{self.config['container_name']}/{key}"
        )