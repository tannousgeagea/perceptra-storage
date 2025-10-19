"""
Advanced usage scenarios for perceptra-storage.

These examples demonstrate:
- Parallel uploads/downloads
- Progress tracking
- Caching strategies
- Multi-backend sync
- Custom adapters
"""
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
import time
import hashlib


def example_parallel_uploads():
    """Upload multiple files in parallel using ThreadPoolExecutor."""
    print("\n=== PARALLEL UPLOADS EXAMPLE ===")
    
    from perceptra_storage import get_storage_adapter
    
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/parallel_demo', 'create_dirs': True}
    )
    
    def upload_single_file(file_info: Tuple[str, bytes]) -> Tuple[bool, str]:
        """Upload a single file (thread-safe)."""
        key, content = file_info
        try:
            adapter.upload_file(BytesIO(content), key)
            return True, key
        except Exception as e:
            return False, f"{key}: {e}"
    
    # Prepare files to upload
    files_to_upload = {
        f'dataset_{i:04d}.csv': f'Dataset {i} content'.encode()
        for i in range(20)
    }
    
    print(f"Uploading {len(files_to_upload)} files in parallel...")
    start_time = time.time()
    
    results = {'success': 0, 'failed': 0}
    
    # Upload in parallel with 5 workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(upload_single_file, item): item[0]
            for item in files_to_upload.items()
        }
        
        for future in as_completed(futures):
            success, result = future.result()
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
                print(f"  ✗ {result}")
    
    elapsed = time.time() - start_time
    
    print(f"\n✓ Results:")
    print(f"  - Uploaded: {results['success']}")
    print(f"  - Failed: {results['failed']}")
    print(f"  - Time: {elapsed:.2f}s")
    print(f"  - Rate: {results['success']/elapsed:.1f} files/sec")


def example_progress_tracking():
    """Upload/download with progress tracking using tqdm."""
    print("\n=== PROGRESS TRACKING EXAMPLE ===")
    
    try:
        from tqdm import tqdm
    except ImportError:
        print("✗ Install tqdm: pip install tqdm")
        return
    
    from perceptra_storage import get_storage_adapter
    
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/progress_demo', 'create_dirs': True}
    )
    
    # Upload multiple files with progress bar
    files = {f'file_{i:03d}.txt': f'Content {i}'.encode() * 100 for i in range(50)}
    
    print("Uploading with progress bar:")
    for key, content in tqdm(files.items(), desc="Uploading"):
        adapter.upload_file(BytesIO(content), key)
        time.sleep(0.01)  # Simulate network delay
    
    print("✓ Upload complete")
    
    # Download with progress
    print("\nDownloading with progress bar:")
    downloaded = []
    for key in tqdm(files.keys(), desc="Downloading"):
        data = adapter.download_file(key)
        downloaded.append(data)
        time.sleep(0.01)
    
    print(f"✓ Downloaded {len(downloaded)} files")


def example_caching_layer():
    """Implement a caching layer for storage operations."""
    print("\n=== CACHING LAYER EXAMPLE ===")
    
    from perceptra_storage import BaseStorageAdapter
    from functools import lru_cache
    
    class CachedStorageAdapter:
        """Wrapper that adds caching to any storage adapter."""
        
        def __init__(self, adapter: BaseStorageAdapter, cache_dir: str = '/tmp/cache'):
            self.adapter = adapter
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.stats = {'hits': 0, 'misses': 0}
        
        def _cache_path(self, key: str) -> Path:
            """Generate cache file path from storage key."""
            key_hash = hashlib.md5(key.encode()).hexdigest()
            return self.cache_dir / key_hash
        
        def download_file(self, key: str) -> bytes:
            """Download with caching."""
            cache_path = self._cache_path(key)
            
            if cache_path.exists():
                self.stats['hits'] += 1
                print(f"  [CACHE HIT] {key}")
                return cache_path.read_bytes()
            
            self.stats['misses'] += 1
            print(f"  [CACHE MISS] {key}")
            
            # Download from storage
            data = self.adapter.download_file(key)
            
            # Cache it
            cache_path.write_bytes(data)
            
            return data
        
        def upload_file(self, file_obj, key: str, **kwargs):
            """Upload and invalidate cache."""
            result = self.adapter.upload_file(file_obj, key, **kwargs)
            
            # Invalidate cache
            cache_path = self._cache_path(key)
            cache_path.unlink(missing_ok=True)
            
            return result
        
        def get_stats(self) -> dict:
            """Get cache statistics."""
            total = self.stats['hits'] + self.stats['misses']
            hit_rate = self.stats['hits'] / total * 100 if total > 0 else 0
            return {
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'hit_rate': f"{hit_rate:.1f}%"
            }
    
    # Demo the caching
    from perceptra_storage import get_storage_adapter
    
    base_adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/cache_demo', 'create_dirs': True}
    )
    
    cached = CachedStorageAdapter(base_adapter)
    
    # Upload a file
    cached.upload_file(BytesIO(b'Cached content'), 'test.txt')
    
    # Download multiple times
    print("\nDownloading same file 5 times:")
    for i in range(5):
        data = cached.download_file('test.txt')
    
    # Show cache stats
    stats = cached.get_stats()
    print(f"\n✓ Cache Statistics:")
    print(f"  - Hits: {stats['hits']}")
    print(f"  - Misses: {stats['misses']}")
    print(f"  - Hit Rate: {stats['hit_rate']}")


def example_multi_backend_sync():
    """Sync files between different storage backends."""
    print("\n=== MULTI-BACKEND SYNC EXAMPLE ===")
    
    from perceptra_storage import get_storage_adapter
    
    # Setup source and destination
    source = get_storage_adapter(
        'local',
        {'base_path': '/tmp/sync_source', 'create_dirs': True}
    )
    
    destination = get_storage_adapter(
        'local',
        {'base_path': '/tmp/sync_dest', 'create_dirs': True}
    )
    
    # Create some files in source
    print("Creating source files...")
    for i in range(5):
        content = f'File {i} content'.encode()
        source.upload_file(BytesIO(content), f'data/file_{i}.txt')
    
    # Sync from source to destination
    print("\nSyncing files...")
    files = source.list_files(prefix='data/')
    
    for file_obj in files:
        # Download from source
        data = source.download_file(file_obj.key)
        
        # Upload to destination
        destination.upload_file(
            BytesIO(data),
            file_obj.key,
            content_type=file_obj.content_type
        )
        
        print(f"  ✓ Synced: {file_obj.key}")
    
    # Verify sync
    dest_files = destination.list_files(prefix='data/')
    print(f"\n✓ Synced {len(dest_files)} files")


def example_custom_adapter():
    """Create and register a custom storage adapter."""
    print("\n=== CUSTOM ADAPTER EXAMPLE ===")
    
    from perceptra_storage import (
        BaseStorageAdapter,
        StorageObject,
        PresignedUrl,
        register_adapter,
        get_storage_adapter
    )
    from datetime import datetime, timezone, timedelta
    
    class InMemoryStorageAdapter(BaseStorageAdapter):
        """Simple in-memory storage adapter for testing."""
        
        def __init__(self, config, credentials=None):
            super().__init__(config, credentials)
            self._storage = {}  # key -> bytes
            self._metadata = {}  # key -> metadata dict
        
        def _validate_config(self):
            # No config required for in-memory
            pass
        
        def test_connection(self, timeout=10):
            return True
        
        def upload_file(self, file_obj, key, content_type=None, metadata=None):
            data = file_obj.read()
            self._storage[key] = data
            self._metadata[key] = {
                'content_type': content_type,
                'metadata': metadata or {},
                'uploaded_at': datetime.now(timezone.utc)
            }
            return key
        
        def download_file(self, key, destination=None):
            if key not in self._storage:
                from perceptra_storage import StorageNotFoundError
                raise StorageNotFoundError(f"Key not found: {key}")
            
            data = self._storage[key]
            
            if destination:
                Path(destination).write_bytes(data)
            
            return data
        
        def delete_file(self, key):
            if key not in self._storage:
                from perceptra_storage import StorageNotFoundError
                raise StorageNotFoundError(f"Key not found: {key}")
            
            del self._storage[key]
            del self._metadata[key]
            return True
        
        def file_exists(self, key):
            return key in self._storage
        
        def get_file_metadata(self, key):
            if key not in self._storage:
                from perceptra_storage import StorageNotFoundError
                raise StorageNotFoundError(f"Key not found: {key}")
            
            meta = self._metadata[key]
            return StorageObject(
                key=key,
                size=len(self._storage[key]),
                last_modified=meta['uploaded_at'],
                content_type=meta['content_type']
            )
        
        def list_files(self, prefix="", max_results=1000):
            files = []
            for key in self._storage.keys():
                if key.startswith(prefix):
                    files.append(self.get_file_metadata(key))
                if len(files) >= max_results:
                    break
            return files
        
        def generate_presigned_url(self, key, expiration=3600, method="GET"):
            # In-memory doesn't support presigned URLs
            return PresignedUrl(
                url=f"memory://{key}",
                expires_at=datetime.now(timezone.utc) + timedelta(seconds=expiration),
                method=method
            )
    
    # Register the custom adapter
    register_adapter('memory', InMemoryStorageAdapter)
    print("✓ Registered custom in-memory adapter")
    
    # Use it
    adapter = get_storage_adapter('memory', {})
    
    # Test operations
    adapter.upload_file(BytesIO(b'Test data'), 'test.txt')
    print("✓ Uploaded to memory")
    
    data = adapter.download_file('test.txt')
    print(f"✓ Downloaded: {data.decode()}")
    
    files = adapter.list_files()
    print(f"✓ Listed {len(files)} files in memory")


def example_batch_operations():
    """Perform batch operations efficiently."""
    print("\n=== BATCH OPERATIONS EXAMPLE ===")
    
    from perceptra_storage import get_storage_adapter
    
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/batch_demo', 'create_dirs': True}
    )
    
    class BatchUploader:
        """Helper for batch uploads with retry logic."""
        
        def __init__(self, adapter, batch_size=10, max_retries=3):
            self.adapter = adapter
            self.batch_size = batch_size
            self.max_retries = max_retries
        
        def upload_batch(self, files: Dict[str, bytes]) -> Dict[str, any]:
            """Upload files in batches."""
            results = {
                'success': [],
                'failed': [],
                'retried': []
            }
            
            items = list(files.items())
            total_batches = (len(items) + self.batch_size - 1) // self.batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * self.batch_size
                end_idx = start_idx + self.batch_size
                batch = items[start_idx:end_idx]
                
                print(f"  Processing batch {batch_num + 1}/{total_batches}")
                
                for key, content in batch:
                    success = self._upload_with_retry(key, content)
                    
                    if success:
                        results['success'].append(key)
                    else:
                        results['failed'].append(key)
            
            return results
        
        def _upload_with_retry(self, key: str, content: bytes) -> bool:
            """Upload with automatic retry."""
            for attempt in range(self.max_retries):
                try:
                    self.adapter.upload_file(BytesIO(content), key)
                    
                    if attempt > 0:
                        self.results['retried'].append(key)
                    
                    return True
                    
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        print(f"    ✗ Failed after {self.max_retries} attempts: {key}")
                        return False
                    
                    wait_time = 2 ** attempt
                    time.sleep(wait_time)
            
            return False
    
    # Demo batch upload
    files = {
        f'batch/file_{i:04d}.txt': f'Content {i}'.encode()
        for i in range(25)
    }
    
    uploader = BatchUploader(adapter, batch_size=10)
    results = uploader.upload_batch(files)
    
    print(f"\n✓ Batch Upload Results:")
    print(f"  - Success: {len(results['success'])}")
    print(f"  - Failed: {len(results['failed'])}")
    print(f"  - Retried: {len(results['retried'])}")


def example_directory_operations():
    """Work with directory-like structures."""
    print("\n=== DIRECTORY OPERATIONS EXAMPLE ===")
    
    from perceptra_storage import get_storage_adapter
    
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/dir_demo', 'create_dirs': True}
    )
    
    def upload_directory(local_dir: Path, remote_prefix: str):
        """Upload entire directory structure."""
        uploaded = []
        
        for file_path in local_dir.rglob('*'):
            if file_path.is_file():
                relative = file_path.relative_to(local_dir)
                remote_key = f"{remote_prefix}/{relative}"
                
                with open(file_path, 'rb') as f:
                    adapter.upload_file(f, remote_key)
                
                uploaded.append(remote_key)
        
        return uploaded
    
    def list_directory(prefix: str) -> Dict[str, List[str]]:
        """List files grouped by directory."""
        files = adapter.list_files(prefix=prefix)
        
        directories = {}
        for file_obj in files:
            parts = file_obj.key.split('/')
            if len(parts) > 1:
                dir_name = '/'.join(parts[:-1])
                if dir_name not in directories:
                    directories[dir_name] = []
                directories[dir_name].append(parts[-1])
        
        return directories
    
    def delete_directory(prefix: str) -> int:
        """Delete all files with prefix (directory)."""
        files = adapter.list_files(prefix=prefix)
        
        for file_obj in files:
            adapter.delete_file(file_obj.key)
        
        return len(files)
    
    # Demo directory operations
    print("Creating test directory structure...")
    test_files = {
        'project/src/main.py': b'print("Hello")',
        'project/src/utils.py': b'def helper(): pass',
        'project/tests/test_main.py': b'def test(): pass',
        'project/README.md': b'# Project',
    }
    
    for key, content in test_files.items():
        adapter.upload_file(BytesIO(content), key)
    
    # List directories
    dirs = list_directory('project/')
    print("\n✓ Directory structure:")
    for dir_name, files in dirs.items():
        print(f"  {dir_name}/")
        for f in files:
            print(f"    - {f}")
    
    # Delete directory
    deleted = delete_directory('project/')
    print(f"\n✓ Deleted {deleted} files from project/")


def main():
    """Run all advanced examples."""
    print("=" * 60)
    print("PERCEPTRA STORAGE - ADVANCED SCENARIOS")
    print("=" * 60)
    
    try:
        example_parallel_uploads()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_progress_tracking()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_caching_layer()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_multi_backend_sync()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_custom_adapter()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_batch_operations()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    try:
        example_directory_operations()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("Advanced examples completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()