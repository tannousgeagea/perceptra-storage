# Perceptra Storage

A unified storage adapter interface for multi-cloud storage backends in Python.

## Features

- **Unified API**: Single interface for all storage backends
- **Multi-cloud support**: S3, Azure Blob, MinIO, and Local filesystem
- **Type-safe**: Full type hints for better IDE support
- **Easy to extend**: Register custom adapters
- **Production-ready**: Comprehensive error handling and logging
- **Zero dependencies**: Core package has no dependencies; install only what you need

## Installation

```bash
# Install core package (local filesystem only)
pip install perceptra-storage

# Install with S3 support
pip install perceptra-storage[s3]

# Install with Azure support
pip install perceptra-storage[azure]

# Install with MinIO support
pip install perceptra-storage[minio]

# Install with all backends
pip install perceptra-storage[all]
```

## Quick Start

```python
from perceptra_storage import get_storage_adapter

# Create an S3 adapter
config = {
    'bucket_name': 'my-bucket',
    'region': 'us-west-2'
}
credentials = {
    'access_key_id': 'YOUR_ACCESS_KEY',
    'secret_access_key': 'YOUR_SECRET_KEY'
}

adapter = get_storage_adapter('s3', config, credentials)

# Test connection
adapter.test_connection()

# Upload a file
with open('data.csv', 'rb') as f:
    adapter.upload_file(f, 'datasets/data.csv', content_type='text/csv')

# Download a file
data = adapter.download_file('datasets/data.csv')

# List files
files = adapter.list_files(prefix='datasets/')
for file in files:
    print(f"{file.key}: {file.size} bytes")

# Generate presigned URL
url = adapter.generate_presigned_url('datasets/data.csv', expiration=3600)
print(f"Temporary URL: {url.url}")

# Delete a file
adapter.delete_file('datasets/data.csv')
```

## Supported Backends

### Amazon S3

```python
config = {
    'bucket_name': 'my-bucket',
    'region': 'us-west-2'  # optional
}
credentials = {
    'access_key_id': 'YOUR_ACCESS_KEY',
    'secret_access_key': 'YOUR_SECRET_KEY',
    'session_token': 'TOKEN'  # optional, for temporary credentials
}
adapter = get_storage_adapter('s3', config, credentials)
```

### Azure Blob Storage

```python
config = {
    'container_name': 'my-container',
    'account_name': 'mystorageaccount'
}
credentials = {
    'account_key': 'YOUR_ACCOUNT_KEY'
    # OR 'connection_string': 'DefaultEndpointsProtocol=https;...'
    # OR 'sas_token': 'sv=2021-06-08&...'
}
adapter = get_storage_adapter('azure', config, credentials)
```

### MinIO / S3-Compatible

```python
config = {
    'bucket_name': 'my-bucket',
    'endpoint_url': 'play.min.io:9000',
    'secure': True,  # Use HTTPS
    'region': 'us-east-1'  # optional
}
credentials = {
    'access_key': 'YOUR_ACCESS_KEY',
    'secret_key': 'YOUR_SECRET_KEY'
}
adapter = get_storage_adapter('minio', config, credentials)
```

### Local Filesystem

```python
config = {
    'base_path': '/var/perceptra/storage',
    'create_dirs': True  # Auto-create directories
}
adapter = get_storage_adapter('local', config)
```

## API Reference

### BaseStorageAdapter

All adapters implement the following methods:

#### test_connection(timeout: int = 10) -> bool
Test connection to storage backend.

#### upload_file(file_obj, key, content_type=None, metadata=None) -> str
Upload a file to storage.

#### download_file(key, destination=None) -> bytes
Download a file from storage.

#### delete_file(key) -> bool
Delete a file from storage.

#### file_exists(key) -> bool
Check if a file exists.

#### get_file_metadata(key) -> StorageObject
Get metadata about a stored file.

#### list_files(prefix="", max_results=1000) -> list[StorageObject]
List files with optional prefix filter.

#### generate_presigned_url(key, expiration=3600, method="GET") -> PresignedUrl
Generate a presigned URL for temporary access.

#### get_public_url(key) -> Optional[str]
Get public URL for a file (if supported).

## Error Handling

The package defines a hierarchy of exceptions:

```python
from perceptra_storage import (
    StorageError,              # Base exception
    StorageConnectionError,    # Connection failures
    StorageOperationError,     # Operation failures
    StorageNotFoundError,      # File not found
    StoragePermissionError,    # Permission denied
)

try:
    adapter.download_file('missing.txt')
except StorageNotFoundError:
    print("File not found")
except StoragePermissionError:
    print("Access denied")
except StorageError as e:
    print(f"Storage error: {e}")
```

## Advanced Usage

### Custom Adapters

You can register custom storage adapters:

```python
from perceptra_storage import BaseStorageAdapter, register_adapter

class CustomStorageAdapter(BaseStorageAdapter):
    def _validate_config(self):
        # Validation logic
        pass
    
    def test_connection(self, timeout=10):
        # Implementation
        pass
    
    # Implement other required methods...

# Register the adapter
register_adapter('custom', CustomStorageAdapter)

# Use it
adapter = get_storage_adapter('custom', config, credentials)
```

### Context Manager Support

For automatic resource cleanup:

```python
class ManagedAdapter:
    def __init__(self, adapter):
        self.adapter = adapter
    
    def __enter__(self):
        self.adapter.test_connection()
        return self.adapter
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup if needed
        pass

with ManagedAdapter(adapter) as storage:
    storage.upload_file(file_obj, 'data.csv')
```

### Batch Operations

```python
# Upload multiple files
files_to_upload = [
    ('local/file1.txt', 'remote/file1.txt'),
    ('local/file2.txt', 'remote/file2.txt'),
]

for local_path, remote_key in files_to_upload:
    with open(local_path, 'rb') as f:
        adapter.upload_file(f, remote_key)
    print(f"Uploaded {local_path} -> {remote_key}")

# Download multiple files
files_to_download = ['data1.csv', 'data2.csv', 'data3.csv']

for key in files_to_download:
    if adapter.file_exists(key):
        data = adapter.download_file(key)
        with open(f"local_{key}", 'wb') as f:
            f.write(data)
```

## Integration with Django

Use with your Django storage profiles:

```python
from perceptra_storage import get_storage_adapter
from myapp.models import StorageProfile

def get_adapter_for_profile(profile: StorageProfile):
    """Create adapter from Django StorageProfile model."""
    # Retrieve credentials from SecretRef
    credentials = None
    if profile.credential_ref:
        credentials = retrieve_credentials(profile.credential_ref)
    
    return get_storage_adapter(
        backend=profile.backend,
        config=profile.config,
        credentials=credentials
    )

# Usage
profile = StorageProfile.objects.get(tenant=tenant, is_default=True)
adapter = get_adapter_for_profile(profile)
adapter.test_connection()
```

## Testing

The package includes comprehensive tests:

```bash
# Install dev dependencies
pip install perceptra-storage[dev]

# Run tests
pytest

# Run with coverage
pytest --cov=perceptra_storage --cov-report=html

# Run specific backend tests
pytest tests/test_s3.py
pytest tests/test_azure.py
pytest tests/test_minio.py
pytest tests/test_local.py
```

## Security Best Practices

1. **Never hardcode credentials**: Use environment variables or secret management systems
2. **Use IAM roles**: When running on AWS, use IAM roles instead of access keys
3. **Managed identities**: Use Azure Managed Identities when possible
4. **Encrypt at rest**: Enable encryption on your storage backends
5. **Use HTTPS**: Always use secure connections (set `secure=True` for MinIO)
6. **Rotate credentials**: Regularly rotate access keys and tokens
7. **Principle of least privilege**: Grant only necessary permissions

## Performance Tips

1. **Batch operations**: Use list operations instead of checking files individually
2. **Streaming**: For large files, use streaming where possible
3. **Parallel uploads**: Use thread pools for multiple file uploads
4. **Connection pooling**: Reuse adapter instances instead of creating new ones
5. **Presigned URLs**: Use presigned URLs for client-side uploads/downloads

## Logging

The package uses Python's standard logging:

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Or configure specific logger
logger = logging.getLogger('perceptra_storage')
logger.setLevel(logging.INFO)
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Support

- Documentation: https://docs.perceptra.ai/storage
- Issues: https://github.com/tannousgeagea/perceptra-storage/issues
- Email: support@perceptra.ai

## Changelog

### 0.1.0 (2025-10-18)
- Initial release
- Support for S3, Azure, MinIO, and Local storage
- Full test coverage
- Type hints and documentation