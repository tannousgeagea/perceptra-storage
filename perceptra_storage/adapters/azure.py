"""
Azure Blob Storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
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
    Optional:
        - account_url: full blob endpoint (private endpoints, Azurite)
        - endpoint_suffix: e.g. core.chinacloudapi.cn (default core.windows.net)

    Credentials required:
        - account_key: Azure storage account key
        OR
        - connection_string: Full connection string
        OR
        - tenant_id + client_id + client_secret: Entra ID service principal
          (needs "Storage Blob Data Reader" and, for presigned URLs,
          "Storage Blob Delegator": URLs are short-lived user-delegation SAS)
        OR
        - sas_token: Shared Access Signature token. It cannot mint short-lived
          URLs (can_presign is False): serve such blobs through a proxy.
    """

    # A user delegation key is valid for at most 7 days; refresh it well before.
    DELEGATION_KEY_TTL = timedelta(hours=12)

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize Azure storage adapter."""
        super().__init__(config, credentials)
        self._blob_service_client = None
        self._container_client = None
        self._delegation_key = None
        self._delegation_key_expiry = None
        self._initialize_client()

    @property
    def account_url(self) -> str:
        """The blob service endpoint, without a query string."""
        if self.config.get('account_url'):
            return self.config['account_url'].rstrip('/')
        suffix = self.config.get('endpoint_suffix') or 'core.windows.net'
        return f"https://{self.config['account_name']}.blob.{suffix}"

    @property
    def uses_service_principal(self) -> bool:
        return bool(self.credentials) and all(
            self.credentials.get(k) for k in ('tenant_id', 'client_id', 'client_secret')
        )

    @property
    def can_presign(self) -> bool:
        """Only a key or a service principal can mint short-lived SAS URLs."""
        if not self.credentials:
            return False
        if self.uses_service_principal or self.credentials.get('account_key'):
            return True
        return 'AccountKey=' in (self.credentials.get('connection_string') or '')

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
            container_name = self.config['container_name']
            account_url = self.account_url

            # Try connection string first
            if self.credentials and 'connection_string' in self.credentials:
                self._blob_service_client = BlobServiceClient.from_connection_string(
                    self.credentials['connection_string']
                )
            # Try account key
            elif self.credentials and 'account_key' in self.credentials:
                self._blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self.credentials['account_key']
                )
            # Try an Entra ID service principal
            elif self.uses_service_principal:
                try:
                    from azure.identity import ClientSecretCredential
                except ImportError:
                    raise ImportError(
                        "azure-identity is required for service principal credentials. "
                        "Install with: pip install perceptra-storage[azure]"
                    )
                self._blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=ClientSecretCredential(
                        tenant_id=self.credentials['tenant_id'],
                        client_id=self.credentials['client_id'],
                        client_secret=self.credentials['client_secret'],
                    ),
                )
            # Try SAS token
            elif self.credentials and 'sas_token' in self.credentials:
                sas_token = self.credentials['sas_token']
                if not sas_token.startswith('?'):
                    sas_token = f"?{sas_token}"
                self._blob_service_client = BlobServiceClient(
                    account_url=f"{account_url}{sas_token}"
                )
            else:
                raise ValueError(
                    "Azure adapter requires one of: connection_string, account_key, "
                    "tenant_id/client_id/client_secret, or sas_token"
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
        except (ClientAuthenticationError, HttpResponseError) as e:
            # A narrowly scoped credential (container SAS sp=r / sp=rl, a
            # data-reader role) may not read container properties.
            if getattr(e, 'status_code', None) in (401, 403) or isinstance(e, ClientAuthenticationError):
                return self._test_narrow_access(timeout)
            raise StorageConnectionError(f"Azure connection failed: {e}")
        except ServiceRequestError as e:
            raise StorageConnectionError(f"Azure connection failed: {e}")

    # Name that cannot exist: probing it needs only read permission on blobs.
    PROBE_BLOB = "__perceptra_connection_probe__/does-not-exist"

    def _test_narrow_access(self, timeout: int) -> bool:
        """Prove access with the least permission available: listing one blob
        (sp=l), else reading a blob that cannot exist (sp=r) — a 404
        BlobNotFound means the request was authorized; 403 means it was not."""
        container = self.config['container_name']
        try:
            next(iter(self._container_client.list_blobs(results_per_page=1, timeout=timeout)), None)
            logger.info(f"Connected to Azure container (list access): {container}")
            return True
        except Exception:
            pass  # no list permission (e.g. sp=r): try a read

        try:
            self._container_client.get_blob_client(self.PROBE_BLOB).get_blob_properties(timeout=timeout)
            return True  # the probe blob exists after all: readable
        except ResourceNotFoundError as e:
            if getattr(e, 'error_code', None) == 'ContainerNotFound':
                raise StorageConnectionError(f"Azure container '{container}' does not exist")
            logger.info(f"Connected to Azure container (read access): {container}")
            return True
        except (ClientAuthenticationError, HttpResponseError) as e:
            raise StoragePermissionError(
                f"Azure authentication failed: the credential cannot read blobs of container '{container}' "
                f"(a SAS needs at least sp=r, scoped to this container, and must not be expired): {e}"
            )
        except ServiceRequestError as e:
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

            # Allow for clock skew between us and Azure.
            start_time = datetime.now(timezone.utc) - timedelta(minutes=5)
            expiry_time = datetime.now(timezone.utc) + timedelta(seconds=expiration)

            container_name = self.config["container_name"]

            # Encode blob key safely while preserving path separators
            encoded_key = quote(key.lstrip("/"), safe="/")
            base_blob_url = f"{self.account_url}/{container_name}/{encoded_key}"

            if self.uses_service_principal:
                sas_token = generate_blob_sas(
                    account_name=self.config['account_name'],
                    container_name=container_name,
                    blob_name=key.lstrip("/"),
                    user_delegation_key=self._get_delegation_key(expiry_time),
                    permission=permissions,
                    expiry=expiry_time,
                    start=start_time,
                )
                return PresignedUrl(url=f"{base_blob_url}?{sas_token}", expires_at=expiry_time,
                                    method=method.upper())

            if not self.can_presign:
                # Never hand out the stored long-lived SAS as if it were a short-lived URL.
                raise StorageOperationError(
                    "This Azure credential cannot mint short-lived URLs (SAS token only): "
                    "use an account key or a service principal, or serve the blob through a proxy"
                )

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
            
            sas_token = generate_blob_sas(
                account_name=self.config['account_name'],
                container_name=container_name,
                blob_name=key.lstrip("/"),
                account_key=account_key,
                permission=permissions,
                expiry=expiry_time,
                start=start_time
            )

            return PresignedUrl(
                url=f"{base_blob_url}?{sas_token}",
                expires_at=expiry_time,
                method=method.upper()
            )

        except StorageOperationError:
            raise
        except Exception as e:
            raise StorageOperationError(f"Azure SAS URL generation failed: {e}")

    def _get_delegation_key(self, needed_until: datetime):
        """A user delegation key valid past `needed_until` (cached, refreshed early)."""
        now = datetime.now(timezone.utc)
        if (
            self._delegation_key is None
            or self._delegation_key_expiry is None
            or self._delegation_key_expiry < needed_until + timedelta(minutes=5)
        ):
            expiry = max(now + self.DELEGATION_KEY_TTL, needed_until + timedelta(minutes=10))
            self._delegation_key = self._blob_service_client.get_user_delegation_key(
                key_start_time=now - timedelta(minutes=5),
                key_expiry_time=expiry,
            )
            self._delegation_key_expiry = expiry
        return self._delegation_key

    def get_public_url(self, key: str) -> Optional[str]:
        """Get public URL for Azure blob (if container is public)."""
        return f"{self.account_url}/{self.config['container_name']}/{quote(key.lstrip('/'), safe='/')}"