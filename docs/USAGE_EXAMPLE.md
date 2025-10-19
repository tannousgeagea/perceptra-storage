# Perceptra Storage - Complete Usage Examples

## Installation

### 1. Install the perceptra-storage package

```bash
# Development installation from local source
cd perceptra-storage
pip install -e .[all,dev]

# Or install specific backends only
pip install -e .[s3,dev]
```

### 2. Install Django app

Add to your Django `INSTALLED_APPS`:

```python
# settings.py
INSTALLED_APPS = [
    ...
    'storage',
]
```

### 3. Run migrations

```bash
python manage.py makemigrations storage
python manage.py migrate storage
```

## Django Model Usage

### Creating Storage Profiles

```python
from core.models import Tenant
from storage.models import StorageProfile, SecretRef

# Get or create tenant
tenant = Tenant.objects.get(name='acme-corp')

# Create a secret reference for S3 credentials
secret_ref = SecretRef.objects.create(
    tenant=tenant,
    provider='vault',
    path='secret/data/storage/s3-prod',
    key='credentials',
    metadata={'environment': 'production'}
)

# Create S3 storage profile
s3_profile = StorageProfile.objects.create(
    tenant=tenant,
    name='Production S3',
    backend='s3',
    region='us-west-2',
    is_default=True,
    config={
        'bucket_name': 'acme-corp-datasets',
        'region': 'us-west-2'
    },
    credential_ref=secret_ref
)

# Create Azure storage profile
azure_profile = StorageProfile.objects.create(
    tenant=tenant,
    name='Azure Blob Storage',
    backend='azure',
    config={
        'container_name': 'models',
        'account_name': 'acmestorage'
    },
    credential_ref=SecretRef.objects.create(
        tenant=tenant,
        provider='azure_kv',
        path='storage-credentials',
        key='azure_storage'
    )
)

# Create local storage profile (no credentials needed)
local_profile = StorageProfile.objects.create(
    tenant=tenant,
    name='Local Development',
    backend='local',
    config={
        'base_path': '/var/perceptra/storage',
        'create_dirs': True
    }
)
```

### Querying Storage Profiles

```python
from storage.models import StorageProfile

# Get default profile for tenant
default_profile = StorageProfile.objects.get(
    tenant=tenant,
    is_default=True
)

# Get all S3 profiles for tenant
s3_profiles = StorageProfile.objects.filter(
    tenant=tenant,
    backend='s3',
    is_active=True
)

# Get profile by name
profile = StorageProfile.objects.get(
    tenant=tenant,
    name='Production S3'
)
```

## Using Storage Adapters with Django

### Method 1: Using the Service Layer (Recommended)

```python
from storage.services import (
    get_storage_adapter_for_profile,
    get_default_storage_adapter,
    test_storage_profile_connection,
    StorageManager,
)
from storage.models import StorageProfile

# Get adapter for a specific profile
profile = StorageProfile.objects.get(tenant=tenant, name='Production S3')
adapter = get_storage_adapter_for_profile(profile, test_connection=True)

# Upload a file
with open('dataset.csv', 'rb') as f:
    key = adapter.upload_file(
        f,
        'datasets/2025/dataset.csv',
        content_type='text/csv',
        metadata={'uploaded_by': 'user123'}
    )

# Download a file
data = adapter.download_file('datasets/2025/dataset.csv')

# Use default storage for tenant
default_adapter = get_default_storage_adapter(tenant)
if default_adapter:
    default_adapter.upload_file(file_obj, 'data.csv')

# Test connection before saving profile
profile = StorageProfile(...)
success, error = test_storage_profile_connection(profile)
if not success:
    print(f"Connection test failed: {error}")
else:
    profile.save()
```

### Method 2: Using Context Manager

```python
from storage.services import StorageManager
from storage.models import StorageProfile

profile = StorageProfile.objects.get(tenant=tenant, is_default=True)

# Automatic connection testing and cleanup
with StorageManager(profile, test_connection=True) as storage:
    # Upload
    storage.upload_file(file_obj, 'processed/results.json')
    
    # List files
    files = storage.list_files(prefix='processed/')
    for file in files:
        print(f"{file.key}: {file.size} bytes")
    
    # Download
    data = storage.download_file('processed/results.json')
```

## Standalone Package Usage (Without Django)

### S3 Example

```python
from perceptra_storage import get_storage_adapter

config = {
    'bucket_name': 'my-datasets',
    'region': 'eu-central-1'
}

credentials = {
    'access_key_id': 'AKIAIOSFODNN7EXAMPLE',
    'secret_access_key': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
}

adapter = get_storage_adapter('s3', config, credentials)

# Test connection
if adapter.test_connection():
    print("Connected successfully!")

# Upload file
with open('model.pkl', 'rb') as f:
    adapter.upload_file(f, 'models/v1/model.pkl')

# Generate presigned URL for sharing
url = adapter.generate_presigned_url(
    'models/v1/model.pkl',
    expiration=3600  # 1 hour
)
print(f"Share this URL: {url.url}")
```

### Azure Blob Storage Example

```python
config = {
    'container_name': 'ml-models',
    'account_name': 'myazurestorage'
}

credentials = {
    'account_key': 'your-account-key-here'
    # OR use connection string:
    # 'connection_string': 'DefaultEndpointsProtocol=https;...'
}

adapter = get_storage_adapter('azure', config, credentials)

# Upload with metadata
with open('training_results.json', 'rb') as f:
    adapter.upload_file(
        f,
        'experiments/exp-001/results.json',
        content_type='application/json',
        metadata={
            'experiment_id': 'exp-001',
            'timestamp': '2025-10-18T10:00:00Z'
        }
    )

# Get metadata
metadata = adapter.get_file_metadata('experiments/exp-001/results.json')
print(f"File size: {metadata.size} bytes")
print(f"Last modified: {metadata.last_modified}")
print(f"Metadata: {metadata.metadata}")
```

### MinIO Example

```python
config = {
    'bucket_name': 'datasets',
    'endpoint_url': 'minio.example.com:9000',
    'secure': True,  # Use HTTPS
    'region': 'us-east-1'
}

credentials = {
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin'
}

adapter = get_storage_adapter('minio', config, credentials)

# Batch upload
import os
for filename in os.listdir('./images'):
    filepath = os.path.join('./images', filename)
    with open(filepath, 'rb') as f:
        adapter.upload_file(f, f'images/{filename}')
    print(f"Uploaded {filename}")

# List all images
files = adapter.list_files(prefix='images/')
print(f"Total images: {len(files)}")
```

### Local Filesystem Example

```python
config = {
    'base_path': '/mnt/storage/perceptra',
    'create_dirs': True
}

adapter = get_storage_adapter('local', config)

# Upload
with open('report.pdf', 'rb') as f:
    adapter.upload_file(f, 'reports/2025/Q1/report.pdf')

# Check if file exists
if adapter.file_exists('reports/2025/Q1/report.pdf'):
    print("Report uploaded successfully!")

# Download to specific location
adapter.download_file(
    'reports/2025/Q1/report.pdf',
    destination='/tmp/downloaded_report.pdf'
)
```

## Advanced Scenarios

### Batch Operations with Progress Tracking

```python
from pathlib import Path
from tqdm import tqdm

def upload_directory(adapter, local_dir, remote_prefix):
    """Upload all files from a directory."""
    local_path = Path(local_dir)
    files = list(local_path.rglob('*'))
    
    for file_path in tqdm(files, desc="Uploading"):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_path)
            remote_key = f"{remote_prefix}/{relative_path}"
            
            with open(file_path, 'rb') as f:
                adapter.upload_file(f, remote_key)

# Usage
profile = StorageProfile.objects.get(tenant=tenant, is_default=True)
adapter = get_storage_adapter_for_profile(profile)
upload_directory(adapter, './datasets', 'uploaded_datasets')
```

### Parallel Uploads with Threading

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

def upload_file_wrapper(adapter, local_file, remote_key):
    """Wrapper for thread-safe upload."""
    try:
        with open(local_file, 'rb') as f:
            adapter.upload_file(f, remote_key)
        return True, remote_key
    except Exception as e:
        return False, str(e)

def parallel_upload(adapter, files_mapping, max_workers=5):
    """
    Upload multiple files in parallel.
    
    Args:
        adapter: Storage adapter instance
        files_mapping: Dict of {local_path: remote_key}
        max_workers: Maximum number of parallel uploads
    """
    results = {'success': [], 'failed': []}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(upload_file_wrapper, adapter, local, remote): (local, remote)
            for local, remote in files_mapping.items()
        }
        
        for future in as_completed(futures):
            local_file, remote_key = futures[future]
            success, result = future.result()
            
            if success:
                results['success'].append(remote_key)
                print(f"✓ Uploaded: {remote_key}")
            else:
                results['failed'].append((remote_key, result))
                print(f"✗ Failed: {remote_key} - {result}")
    
    return results

# Usage
files_to_upload = {
    '/data/file1.csv': 'datasets/file1.csv',
    '/data/file2.csv': 'datasets/file2.csv',
    '/data/file3.csv': 'datasets/file3.csv',
}

adapter = get_storage_adapter_for_profile(profile)
results = parallel_upload(adapter, files_to_upload, max_workers=3)
print(f"Success: {len(results['success'])}, Failed: {len(results['failed'])}")
```

### Multi-Backend Sync

```python
def sync_between_storages(source_profile, dest_profile, prefix=''):
    """Sync files from one storage backend to another."""
    source_adapter = get_storage_adapter_for_profile(source_profile)
    dest_adapter = get_storage_adapter_for_profile(dest_profile)
    
    # List files in source
    files = source_adapter.list_files(prefix=prefix)
    
    for file_obj in files:
        # Download from source
        data = source_adapter.download_file(file_obj.key)
        
        # Upload to destination
        from io import BytesIO
        dest_adapter.upload_file(
            BytesIO(data),
            file_obj.key,
            content_type=file_obj.content_type
        )
        print(f"Synced: {file_obj.key}")

# Usage: Sync from S3 to Azure
s3_profile = StorageProfile.objects.get(tenant=tenant, backend='s3')
azure_profile = StorageProfile.objects.get(tenant=tenant, backend='azure')
sync_between_storages(s3_profile, azure_profile, prefix='datasets/')
```

### Handling Large Files with Streaming

```python
def download_large_file(adapter, key, destination, chunk_size=8192):
    """Download large file with progress tracking."""
    from tqdm import tqdm
    
    # Get file metadata
    metadata = adapter.get_file_metadata(key)
    total_size = metadata.size
    
    # Download in chunks
    data = adapter.download_file(key)
    
    with open(destination, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
            f.write(data)
            pbar.update(len(data))
    
    print(f"Downloaded {key} to {destination}")

# Usage
adapter = get_storage_adapter_for_profile(profile)
download_large_file(adapter, 'models/large_model.pkl', '/tmp/model.pkl')
```

### Implementing Caching Layer

```python
from functools import lru_cache
from hashlib import md5

class CachedStorageAdapter:
    """Storage adapter with local caching."""
    
    def __init__(self, adapter, cache_dir='/tmp/storage_cache'):
        self.adapter = adapter
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _cache_key(self, key):
        """Generate cache filename from storage key."""
        return self.cache_dir / md5(key.encode()).hexdigest()
    
    def download_file(self, key):
        """Download with caching."""
        cache_file = self._cache_key(key)
        
        if cache_file.exists():
            print(f"Cache hit: {key}")
            return cache_file.read_bytes()
        
        print(f"Cache miss: {key}")
        data = self.adapter.download_file(key)
        cache_file.write_bytes(data)
        return data
    
    def upload_file(self, file_obj, key, **kwargs):
        """Upload and invalidate cache."""
        result = self.adapter.upload_file(file_obj, key, **kwargs)
        
        # Invalidate cache
        cache_file = self._cache_key(key)
        cache_file.unlink(missing_ok=True)
        
        return result

# Usage
adapter = get_storage_adapter_for_profile(profile)
cached_adapter = CachedStorageAdapter(adapter)

# First download - cache miss
data1 = cached_adapter.download_file('data.csv')

# Second download - cache hit (faster)
data2 = cached_adapter.download_file('data.csv')
```

## Celery Task Integration

```python
# tasks.py
from celery import shared_task
from storage.services import get_storage_adapter_for_profile
from storage.models import StorageProfile

@shared_task(bind=True, max_retries=3)
def upload_file_task(self, profile_id, file_path, remote_key):
    """Celery task for async file upload."""
    try:
        profile = StorageProfile.objects.get(id=profile_id)
        adapter = get_storage_adapter_for_profile(profile)
        
        with open(file_path, 'rb') as f:
            adapter.upload_file(f, remote_key)
        
        return {'status': 'success', 'key': remote_key}
        
    except Exception as e:
        # Retry with exponential backoff
        self.retry(exc=e, countdown=2 ** self.request.retries)

@shared_task
def cleanup_old_files(profile_id, days_old=30):
    """Delete files older than specified days."""
    from datetime import datetime, timedelta, timezone
    
    profile = StorageProfile.objects.get(id=profile_id)
    adapter = get_storage_adapter_for_profile(profile)
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
    files = adapter.list_files()
    
    deleted_count = 0
    for file_obj in files:
        if file_obj.last_modified < cutoff_date:
            adapter.delete_file(file_obj.key)
            deleted_count += 1
    
    return {'deleted': deleted_count}

# Usage in views
from .tasks import upload_file_task

def upload_view(request):
    profile = request.user.tenant.storage_profiles.get(is_default=True)
    file_path = '/tmp/uploaded_file.csv'
    
    # Queue task
    task = upload_file_task.delay(str(profile.id), file_path, 'data.csv')
    
    return JsonResponse({'task_id': task.id})
```

## Django REST API Views

```python
# views.py
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from storage.models import StorageProfile
from storage.services import (
    get_storage_adapter_for_profile,
    test_storage_profile_connection,
)

class StorageProfileViewSet(viewsets.ModelViewSet):
    """API endpoints for storage profiles."""
    
    def get_queryset(self):
        return StorageProfile.objects.filter(
            tenant=self.request.user.tenant,
            is_active=True
        )
    
    @action(detail=True, methods=['post'])
    def test_connection(self, request, pk=None):
        """Test storage profile connection."""
        profile = self.get_object()
        success, error = test_storage_profile_connection(profile)
        
        if success:
            return Response({'status': 'connected'})
        return Response(
            {'status': 'failed', 'error': error},
            status=status.HTTP_400_BAD_REQUEST
        )
    
    @action(detail=True, methods=['get'])
    def list_files(self, request, pk=None):
        """List files in storage."""
        profile = self.get_object()
        prefix = request.query_params.get('prefix', '')
        
        try:
            adapter = get_storage_adapter_for_profile(profile)
            files = adapter.list_files(prefix=prefix, max_results=100)
            
            return Response({
                'files': [
                    {
                        'key': f.key,
                        'size': f.size,
                        'last_modified': f.last_modified.isoformat(),
                    }
                    for f in files
                ]
            })
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=True, methods=['post'])
    def generate_upload_url(self, request, pk=None):
        """Generate presigned URL for client-side upload."""
        profile = self.get_object()
        key = request.data.get('key')
        
        if not key:
            return Response(
                {'error': 'key is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            adapter = get_storage_adapter_for_profile(profile)
            presigned = adapter.generate_presigned_url(
                key,
                expiration=3600,
                method='PUT'
            )
            
            return Response({
                'url': presigned.url,
                'expires_at': presigned.expires_at.isoformat(),
                'method': presigned.method
            })
        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
```

## Error Handling Best Practices

```python
from perceptra_storage import (
    StorageError,
    StorageConnectionError,
    StorageNotFoundError,
    StoragePermissionError,
    StorageOperationError,
)
import logging

logger = logging.getLogger(__name__)

def safe_upload(adapter, file_obj, key, max_retries=3):
    """Upload with retry logic and error handling."""
    for attempt in range(max_retries):
        try:
            result = adapter.upload_file(file_obj, key)
            logger.info(f"Upload successful: {key}")
            return result
            
        except StoragePermissionError as e:
            logger.error(f"Permission denied: {e}")
            raise  # Don't retry permission errors
            
        except StorageConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection failed, retrying... ({attempt + 1}/{max_retries})")
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            logger.error(f"Connection failed after {max_retries} attempts")
            raise
            
        except StorageOperationError as e:
            logger.error(f"Operation failed: {e}")
            raise
            
        except StorageError as e:
            logger.error(f"Unexpected storage error: {e}")
            raise

# Usage
try:
    safe_upload(adapter, file_obj, 'important_data.csv')
except StorageError as e:
    # Handle or notify
    send_alert(f"Upload failed: {e}")
```

## Testing Your Storage Integration

```python
# tests/test_storage_integration.py
import pytest
from storage.models import StorageProfile
from storage.services import get_storage_adapter_for_profile
from io import BytesIO

@pytest.mark.django_db
class TestStorageIntegration:
    
    def test_upload_download_cycle(self, tenant, local_storage_profile):
        """Test complete upload/download cycle."""
        adapter = get_storage_adapter_for_profile(local_storage_profile)
        
        # Upload
        content = b'Test data'
        adapter.upload_file(BytesIO(content), 'test.txt')
        
        # Download
        downloaded = adapter.download_file('test.txt')
        assert downloaded == content
        
        # Cleanup
        adapter.delete_file('test.txt')
    
    def test_profile_validation(self, tenant):
        """Test that invalid profiles are rejected."""
        with pytest.raises(ValueError):
            StorageProfile.objects.create(
                tenant=tenant,
                name='Invalid',
                backend='s3',
                config={}  # Missing required bucket_name
            )
```

## Migration from Existing Storage

```python
def migrate_from_old_storage(old_path, new_profile, prefix='migrated/'):
    """Migrate files from old storage system."""
    from pathlib import Path
    
    adapter = get_storage_adapter_for_profile(new_profile)
    old_storage = Path(old_path)
    
    migrated_count = 0
    failed_count = 0
    
    for file_path in old_storage.rglob('*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(old_storage)
            remote_key = f"{prefix}{relative_path}"
            
            try:
                with open(file_path, 'rb') as f:
                    adapter.upload_file(f, remote_key)
                migrated_count += 1
                print(f"✓ Migrated: {relative_path}")
            except Exception as e:
                failed_count += 1
                print(f"✗ Failed: {relative_path} - {e}")
    
    return {'migrated': migrated_count, 'failed': failed_count}

# Usage
profile = StorageProfile.objects.get(tenant=tenant, name='New S3 Storage')
results = migrate_from_old_storage('/old/storage/path', profile)
print(f"Migration complete: {results}")
```

This comprehensive guide covers all major use cases for the storage management system!