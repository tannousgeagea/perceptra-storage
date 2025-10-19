"""
Unit tests for base storage adapter functionality.
"""
import pytest
from datetime import datetime, timezone
from perceptra_storage.base import (
    BaseStorageAdapter,
    StorageObject,
    PresignedUrl,
    StorageError,
    StorageConnectionError,
    StorageOperationError,
    StorageNotFoundError,
    StoragePermissionError,
)


class TestStorageExceptions:
    """Test storage exception hierarchy."""
    
    def test_base_exception(self):
        """Test base StorageError exception."""
        exc = StorageError("Test error")
        assert str(exc) == "Test error"
        assert isinstance(exc, Exception)
    
    def test_connection_error(self):
        """Test StorageConnectionError inherits from StorageError."""
        exc = StorageConnectionError("Connection failed")
        assert isinstance(exc, StorageError)
        assert str(exc) == "Connection failed"
    
    def test_operation_error(self):
        """Test StorageOperationError inherits from StorageError."""
        exc = StorageOperationError("Operation failed")
        assert isinstance(exc, StorageError)
    
    def test_not_found_error(self):
        """Test StorageNotFoundError inherits from StorageError."""
        exc = StorageNotFoundError("File not found")
        assert isinstance(exc, StorageError)
    
    def test_permission_error(self):
        """Test StoragePermissionError inherits from StorageError."""
        exc = StoragePermissionError("Access denied")
        assert isinstance(exc, StorageError)


class TestStorageObject:
    """Test StorageObject dataclass."""
    
    def test_storage_object_creation(self):
        """Test creating a StorageObject instance."""
        now = datetime.now(timezone.utc)
        obj = StorageObject(
            key="path/to/file.txt",
            size=1024,
            last_modified=now,
            etag="abc123",
            content_type="text/plain",
            metadata={"custom": "value"}
        )
        
        assert obj.key == "path/to/file.txt"
        assert obj.size == 1024
        assert obj.last_modified == now
        assert obj.etag == "abc123"
        assert obj.content_type == "text/plain"
        assert obj.metadata == {"custom": "value"}
    
    def test_storage_object_optional_fields(self):
        """Test StorageObject with optional fields."""
        now = datetime.now(timezone.utc)
        obj = StorageObject(
            key="file.txt",
            size=100,
            last_modified=now
        )
        
        assert obj.etag is None
        assert obj.content_type is None
        assert obj.metadata is None


class TestPresignedUrl:
    """Test PresignedUrl dataclass."""
    
    def test_presigned_url_creation(self):
        """Test creating a PresignedUrl instance."""
        now = datetime.now(timezone.utc)
        url = PresignedUrl(
            url="https://example.com/file?signature=xyz",
            expires_at=now,
            method="GET"
        )
        
        assert url.url == "https://example.com/file?signature=xyz"
        assert url.expires_at == now
        assert url.method == "GET"
    
    def test_presigned_url_default_method(self):
        """Test PresignedUrl with default method."""
        now = datetime.now(timezone.utc)
        url = PresignedUrl(
            url="https://example.com/file",
            expires_at=now
        )
        
        assert url.method == "GET"


class TestBaseStorageAdapter:
    """Test BaseStorageAdapter abstract class."""
    
    def test_cannot_instantiate_directly(self):
        """Test that BaseStorageAdapter cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseStorageAdapter({}, {})
    
    def test_repr_hides_credentials(self):
        """Test that __repr__ doesn't expose credentials."""
        class DummyAdapter(BaseStorageAdapter):
            def _validate_config(self):
                pass
            def test_connection(self, timeout=10):
                return True
            def upload_file(self, file_obj, key, content_type=None, metadata=None):
                return key
            def download_file(self, key, destination=None):
                return b''
            def delete_file(self, key):
                return True
            def file_exists(self, key):
                return False
            def get_file_metadata(self, key):
                pass
            def list_files(self, prefix="", max_results=1000):
                return []
            def generate_presigned_url(self, key, expiration=3600, method="GET"):
                pass
        
        config = {'bucket': 'test', 'key': 'value'}
        credentials = {'secret': 'should_not_appear'}
        adapter = DummyAdapter(config, credentials)
        
        repr_str = repr(adapter)
        assert 'DummyAdapter' in repr_str
        assert 'bucket' in repr_str or 'key' in repr_str
        assert 'should_not_appear' not in repr_str
        assert 'secret' not in repr_str
    
    def test_get_public_url_default(self):
        """Test default get_public_url returns None."""
        class DummyAdapter(BaseStorageAdapter):
            def _validate_config(self):
                pass
            def test_connection(self, timeout=10):
                return True
            def upload_file(self, file_obj, key, content_type=None, metadata=None):
                return key
            def download_file(self, key, destination=None):
                return b''
            def delete_file(self, key):
                return True
            def file_exists(self, key):
                return False
            def get_file_metadata(self, key):
                pass
            def list_files(self, prefix="", max_results=1000):
                return []
            def generate_presigned_url(self, key, expiration=3600, method="GET"):
                pass
        
        adapter = DummyAdapter({}, {})
        assert adapter.get_public_url('test.txt') is None