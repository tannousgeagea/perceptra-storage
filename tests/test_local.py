"""
Unit tests for local filesystem storage adapter.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from io import BytesIO
from perceptra_storage import (
    LocalStorageAdapter,
    StorageConnectionError,
    StorageNotFoundError,
    StorageOperationError,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def local_adapter(temp_dir):
    """Create a local storage adapter instance."""
    config = {'base_path': temp_dir, 'create_dirs': True}
    return LocalStorageAdapter(config)


class TestLocalAdapterInitialization:
    """Test local adapter initialization."""
    
    def test_init_with_valid_config(self, temp_dir):
        """Test initialization with valid configuration."""
        config = {'base_path': temp_dir}
        adapter = LocalStorageAdapter(config)
        assert adapter._base_path == Path(temp_dir).resolve()
    
    def test_init_creates_directory(self):
        """Test that initialization creates base directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir) / 'new_storage'
            config = {'base_path': str(base_path), 'create_dirs': True}
            
            adapter = LocalStorageAdapter(config)
            assert base_path.exists()
            assert base_path.is_dir()
    
    def test_init_without_create_dirs(self):
        """Test initialization fails if directory doesn't exist and create_dirs=False."""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = Path(temp_dir) / 'nonexistent'
            config = {'base_path': str(base_path), 'create_dirs': False}
            
            with pytest.raises(StorageConnectionError):
                LocalStorageAdapter(config)
    
    def test_init_missing_base_path(self):
        """Test initialization fails without base_path."""
        with pytest.raises(ValueError) as exc_info:
            LocalStorageAdapter({})
        assert 'base_path' in str(exc_info.value)
    
    def test_init_empty_base_path(self):
        """Test initialization fails with empty base_path."""
        with pytest.raises(ValueError):
            LocalStorageAdapter({'base_path': ''})
    
    def test_init_base_path_is_file(self, temp_dir):
        """Test initialization fails if base_path is a file."""
        file_path = Path(temp_dir) / 'file.txt'
        file_path.write_text('test')
        
        with pytest.raises(StorageConnectionError):
            LocalStorageAdapter({'base_path': str(file_path)})


class TestLocalAdapterConnection:
    """Test local adapter connection testing."""
    
    def test_connection_success(self, local_adapter):
        """Test successful connection test."""
        assert local_adapter.test_connection() is True
    
    def test_connection_creates_test_file(self, local_adapter, temp_dir):
        """Test that connection test doesn't leave test file."""
        local_adapter.test_connection()
        test_file = Path(temp_dir) / '.storage_test'
        assert not test_file.exists()


class TestLocalAdapterUpload:
    """Test local adapter file upload."""
    
    def test_upload_file(self, local_adapter):
        """Test uploading a file."""
        content = b'Hello, World!'
        file_obj = BytesIO(content)
        
        key = local_adapter.upload_file(file_obj, 'test.txt')
        assert key == 'test.txt'
        
        # Verify file was created
        assert local_adapter.file_exists('test.txt')
    
    def test_upload_with_subdirectory(self, local_adapter):
        """Test uploading to subdirectory."""
        content = b'Test data'
        file_obj = BytesIO(content)
        
        key = local_adapter.upload_file(file_obj, 'subdir/file.txt')
        assert key == 'subdir/file.txt'
        assert local_adapter.file_exists('subdir/file.txt')
    
    def test_upload_with_metadata(self, local_adapter, temp_dir):
        """Test uploading with metadata."""
        content = b'Data with metadata'
        file_obj = BytesIO(content)
        metadata = {'author': 'test', 'version': '1.0'}
        
        local_adapter.upload_file(
            file_obj,
            'meta_test.txt',
            content_type='text/plain',
            metadata=metadata
        )
        
        # Check metadata file was created
        meta_file = Path(temp_dir) / 'meta_test.txt.meta'
        assert meta_file.exists()
    
    def test_upload_overwrites_existing(self, local_adapter):
        """Test that upload overwrites existing file."""
        file1 = BytesIO(b'Version 1')
        local_adapter.upload_file(file1, 'overwrite.txt')
        
        file2 = BytesIO(b'Version 2')
        local_adapter.upload_file(file2, 'overwrite.txt')
        
        downloaded = local_adapter.download_file('overwrite.txt')
        assert downloaded == b'Version 2'


class TestLocalAdapterDownload:
    """Test local adapter file download."""
    
    def test_download_file(self, local_adapter):
        """Test downloading a file."""
        content = b'Download test'
        local_adapter.upload_file(BytesIO(content), 'download.txt')
        
        downloaded = local_adapter.download_file('download.txt')
        assert downloaded == content
    
    def test_download_to_destination(self, local_adapter):
        """Test downloading to specific destination."""
        content = b'Destination test'
        local_adapter.upload_file(BytesIO(content), 'dest.txt')
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            dest_path = Path(tmp.name)
        
        try:
            downloaded = local_adapter.download_file('dest.txt', dest_path)
            assert downloaded == content
            assert dest_path.exists()
            assert dest_path.read_bytes() == content
        finally:
            dest_path.unlink(missing_ok=True)
    
    def test_download_nonexistent_file(self, local_adapter):
        """Test downloading nonexistent file raises error."""
        with pytest.raises(StorageNotFoundError):
            local_adapter.download_file('nonexistent.txt')


class TestLocalAdapterDelete:
    """Test local adapter file deletion."""
    
    def test_delete_file(self, local_adapter):
        """Test deleting a file."""
        local_adapter.upload_file(BytesIO(b'Delete me'), 'delete.txt')
        
        assert local_adapter.file_exists('delete.txt')
        result = local_adapter.delete_file('delete.txt')
        assert result is True
        assert not local_adapter.file_exists('delete.txt')
    
    def test_delete_with_metadata(self, local_adapter, temp_dir):
        """Test deleting file also deletes metadata."""
        local_adapter.upload_file(
            BytesIO(b'With meta'),
            'with_meta.txt',
            metadata={'key': 'value'}
        )
        
        local_adapter.delete_file('with_meta.txt')
        
        meta_file = Path(temp_dir) / 'with_meta.txt.meta'
        assert not meta_file.exists()
    
    def test_delete_nonexistent_file(self, local_adapter):
        """Test deleting nonexistent file raises error."""
        with pytest.raises(StorageNotFoundError):
            local_adapter.delete_file('nonexistent.txt')


class TestLocalAdapterMetadata:
    """Test local adapter metadata operations."""
    
    def test_get_file_metadata(self, local_adapter):
        """Test getting file metadata."""
        content = b'Metadata test'
        local_adapter.upload_file(BytesIO(content), 'meta.txt')
        
        metadata = local_adapter.get_file_metadata('meta.txt')
        
        assert metadata.key == 'meta.txt'
        assert metadata.size == len(content)
        assert metadata.etag is not None
        assert metadata.last_modified is not None
    
    def test_file_exists_true(self, local_adapter):
        """Test file_exists returns True for existing file."""
        local_adapter.upload_file(BytesIO(b'Exists'), 'exists.txt')
        assert local_adapter.file_exists('exists.txt') is True
    
    def test_file_exists_false(self, local_adapter):
        """Test file_exists returns False for nonexistent file."""
        assert local_adapter.file_exists('nonexistent.txt') is False


class TestLocalAdapterListing:
    """Test local adapter file listing."""
    
    def test_list_files(self, local_adapter):
        """Test listing files."""
        # Upload test files
        for i in range(3):
            local_adapter.upload_file(BytesIO(f'File {i}'.encode()), f'file{i}.txt')
        
        files = local_adapter.list_files()
        
        assert len(files) == 3
        keys = [f.key for f in files]
        assert 'file0.txt' in keys
        assert 'file1.txt' in keys
        assert 'file2.txt' in keys
    
    def test_list_files_with_prefix(self, local_adapter):
        """Test listing files with prefix filter."""
        local_adapter.upload_file(BytesIO(b'A'), 'prefix_a.txt')
        local_adapter.upload_file(BytesIO(b'B'), 'prefix_b.txt')
        local_adapter.upload_file(BytesIO(b'C'), 'other.txt')
        
        files = local_adapter.list_files(prefix='prefix_')
        
        assert len(files) == 2
        keys = [f.key for f in files]
        assert 'prefix_a.txt' in keys
        assert 'prefix_b.txt' in keys
        assert 'other.txt' not in keys
    
    def test_list_files_max_results(self, local_adapter):
        """Test listing files respects max_results."""
        for i in range(10):
            local_adapter.upload_file(BytesIO(f'File {i}'.encode()), f'file{i}.txt')
        
        files = local_adapter.list_files(max_results=5)
        assert len(files) == 5
    
    def test_list_empty_directory(self, local_adapter):
        """Test listing empty directory returns empty list."""
        files = local_adapter.list_files()
        assert files == []


class TestLocalAdapterPresignedUrl:
    """Test local adapter presigned URL generation."""
    
    def test_generate_presigned_url(self, local_adapter):
        """Test generating presigned URL."""
        local_adapter.upload_file(BytesIO(b'URL test'), 'url_test.txt')
        
        presigned = local_adapter.generate_presigned_url('url_test.txt')
        
        assert presigned.url.startswith('file://')
        assert presigned.method == 'GET'
        assert presigned.expires_at is not None
    
    def test_generate_presigned_url_nonexistent(self, local_adapter):
        """Test generating presigned URL for nonexistent file."""
        with pytest.raises(StorageNotFoundError):
            local_adapter.generate_presigned_url('nonexistent.txt')
    
    def test_get_public_url(self, local_adapter):
        """Test getting public URL."""
        local_adapter.upload_file(BytesIO(b'Public'), 'public.txt')
        
        url = local_adapter.get_public_url('public.txt')
        assert url is not None
        assert url.startswith('file://')
    
    def test_get_public_url_nonexistent(self, local_adapter):
        """Test getting public URL for nonexistent file."""
        url = local_adapter.get_public_url('nonexistent.txt')
        assert url is None


class TestLocalAdapterSecurity:
    """Test local adapter security features."""
    
    def test_path_traversal_prevention(self, local_adapter):
        """Test that path traversal attacks are prevented."""
        with pytest.raises(ValueError):
            local_adapter.upload_file(BytesIO(b'Bad'), '../../../etc/passwd')
    
    def test_absolute_path_prevention(self, local_adapter):
        """Test that absolute paths are prevented."""
        with pytest.raises(ValueError):
            local_adapter.upload_file(BytesIO(b'Bad'), '/tmp/malicious.txt')
    
    def test_double_dot_prevention(self, local_adapter):
        """Test that .. in path is prevented."""
        with pytest.raises(ValueError):
            local_adapter.upload_file(BytesIO(b'Bad'), 'subdir/../../../bad.txt')