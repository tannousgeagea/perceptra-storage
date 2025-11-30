"""
Remote server storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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


class RemoteStorageAdapter(BaseStorageAdapter):
    """
    Remote server storage adapter for on-premise or custom servers.
    
    Configuration required:
        - base_url: Base URL of remote storage server (e.g., 'https://storage.company.com')
        - timeout: Request timeout in seconds (default: 30)
        - verify_ssl: Whether to verify SSL certificates (default: True)
        - max_retries: Maximum number of retry attempts (default: 3)
    
    Credentials required:
        - api_key: API key for authentication
        OR
        - username: Username for basic auth
        - password: Password for basic auth
        OR
        - token: Bearer token for authentication
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize remote storage adapter."""
        super().__init__(config, credentials)
        self._session = None
        self._initialize_session()

    def _validate_config(self) -> None:
        """Validate remote server configuration."""
        if 'base_url' not in self.config:
            raise ValueError("Remote adapter requires 'base_url' in config")
        
        if not self.config['base_url']:
            raise ValueError("base_url cannot be empty")
        
        # Ensure base_url doesn't end with slash
        self.config['base_url'] = self.config['base_url'].rstrip('/')

    def _initialize_session(self) -> None:
        """Initialize requests session with retry logic and authentication."""
        try:
            self._session = requests.Session()
            
            # Configure retry strategy
            max_retries = self.config.get('max_retries', 3)
            retry_strategy = Retry(
                total=max_retries,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "PUT", "DELETE", "POST"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
            
            # Set authentication
            if self.credentials:
                if 'api_key' in self.credentials:
                    self._session.headers['X-API-Key'] = self.credentials['api_key']
                elif 'token' in self.credentials:
                    self._session.headers['Authorization'] = f"Bearer {self.credentials['token']}"
                elif 'username' in self.credentials and 'password' in self.credentials:
                    self._session.auth = (
                        self.credentials['username'],
                        self.credentials['password']
                    )
            
            # Set default headers
            self._session.headers['User-Agent'] = 'perceptra-storage/1.0'
            
        except Exception as e:
            logger.error(f"Failed to initialize remote storage session: {e}")
            raise StorageConnectionError(f"Remote storage initialization failed: {e}")

    def _get_timeout(self) -> int:
        """Get request timeout from config."""
        return self.config.get('timeout', 30)

    def _verify_ssl(self) -> bool:
        """Get SSL verification setting from config."""
        return self.config.get('verify_ssl', True)

    def _build_url(self, endpoint: str) -> str:
        """Build full URL for endpoint."""
        return f"{self.config['base_url']}/{endpoint.lstrip('/')}"

    def _handle_response(self, response: requests.Response, operation: str) -> None:
        """Handle HTTP response and raise appropriate exceptions."""
        if response.status_code == 404:
            raise StorageNotFoundError(f"Resource not found: {response.url}")
        elif response.status_code == 403:
            raise StoragePermissionError(f"Permission denied: {operation}")
        elif response.status_code >= 400:
            raise StorageOperationError(
                f"{operation} failed with status {response.status_code}: {response.text}"
            )

    def test_connection(self, timeout: int = 10) -> bool:
        """Test remote server connection."""
        try:
            url = self._build_url('/health')
            response = self._session.get(
                url,
                timeout=timeout,
                verify=self._verify_ssl()
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully connected to remote storage: {self.config['base_url']}")
                return True
            else:
                raise StorageConnectionError(
                    f"Remote server health check failed with status {response.status_code}"
                )
                
        except requests.exceptions.ConnectionError as e:
            raise StorageConnectionError(f"Cannot connect to remote server: {e}")
        except requests.exceptions.Timeout as e:
            raise StorageConnectionError(f"Connection timeout to remote server: {e}")
        except Exception as e:
            raise StorageConnectionError(f"Remote connection test failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to remote server."""
        try:
            url = self._build_url(f'/files/{key}')
            
            files = {'file': (key, file_obj, content_type or 'application/octet-stream')}
            data = {}
            
            if metadata:
                data['metadata'] = metadata
            
            response = self._session.put(
                url,
                files=files,
                data=data,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            self._handle_response(response, 'upload')
            logger.info(f"Uploaded file to remote server: {key}")
            return key
            
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from remote server."""
        try:
            url = self._build_url(f'/files/{key}')
            
            response = self._session.get(
                url,
                timeout=self._get_timeout(),
                verify=self._verify_ssl(),
                stream=True
            )
            
            self._handle_response(response, 'download')
            
            if destination:
                destination = Path(destination)
                destination.parent.mkdir(parents=True, exist_ok=True)
                
                with open(destination, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Downloaded remote file to: {destination}")
                with open(destination, 'rb') as f:
                    return f.read()
            else:
                data = response.content
                logger.info(f"Downloaded remote file: {key}")
                return data
                
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from remote server."""
        try:
            url = self._build_url(f'/files/{key}')
            
            response = self._session.delete(
                url,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            self._handle_response(response, 'delete')
            logger.info(f"Deleted remote file: {key}")
            return True
            
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists on remote server."""
        try:
            url = self._build_url(f'/files/{key}')
            
            response = self._session.head(
                url,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            return response.status_code == 200
            
        except Exception as e:
            raise StorageOperationError(f"Remote file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from remote server."""
        try:
            url = self._build_url(f'/files/{key}/metadata')
            
            response = self._session.get(
                url,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            self._handle_response(response, 'get_metadata')
            
            data = response.json()
            
            return StorageObject(
                key=key,
                size=data['size'],
                last_modified=datetime.fromisoformat(data['last_modified']),
                etag=data.get('etag'),
                content_type=data.get('content_type'),
                metadata=data.get('metadata')
            )
            
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote metadata retrieval failed: {e}")

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files on remote server."""
        try:
            url = self._build_url('/files')
            
            params = {
                'max_results': max_results
            }
            if prefix:
                params['prefix'] = prefix
            
            response = self._session.get(
                url,
                params=params,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            self._handle_response(response, 'list')
            
            data = response.json()
            files = []
            
            for item in data.get('files', []):
                files.append(StorageObject(
                    key=item['key'],
                    size=item['size'],
                    last_modified=datetime.fromisoformat(item['last_modified']),
                    etag=item.get('etag'),
                    content_type=item.get('content_type'),
                    metadata=item.get('metadata')
                ))
            
            logger.info(f"Listed {len(files)} files from remote server with prefix: {prefix}")
            return files
            
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """Generate presigned URL from remote server."""
        try:
            url = self._build_url(f'/files/{key}/presigned')
            
            params = {
                'expiration': expiration,
                'method': method.upper()
            }
            
            response = self._session.post(
                url,
                json=params,
                timeout=self._get_timeout(),
                verify=self._verify_ssl()
            )
            
            self._handle_response(response, 'generate_presigned_url')
            
            data = response.json()
            
            return PresignedUrl(
                url=data['url'],
                expires_at=datetime.fromisoformat(data['expires_at']),
                method=method.upper()
            )
            
        except (StorageNotFoundError, StoragePermissionError, StorageOperationError):
            raise
        except Exception as e:
            raise StorageOperationError(f"Remote presigned URL generation failed: {e}")

    def get_public_url(self, key: str) -> Optional[str]:
        """Get public URL for remote file if supported."""
        return f"{self.config['base_url']}/public/{key}"

    def __del__(self):
        """Close session on cleanup."""
        if self._session:
            self._session.close()