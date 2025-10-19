"""
Unit tests for storage adapter factory.
"""
import pytest
from perceptra_storage import (
    get_storage_adapter,
    register_adapter,
    list_available_backends,
    get_adapter_info,
    StorageAdapterError,
    BaseStorageAdapter,
    S3StorageAdapter,
    AzureStorageAdapter,
    MinIOStorageAdapter,
    LocalStorageAdapter,
)


class TestGetStorageAdapter:
    """Test get_storage_adapter factory function."""
    
    def test_create_s3_adapter(self):
        """Test creating S3 adapter."""
        config = {'bucket_name': 'test-bucket', 'region': 'us-east-1'}
        credentials = {'access_key_id': 'key', 'secret_access_key': 'secret'}
        
        adapter = get_storage_adapter('s3', config, credentials)
        assert isinstance(adapter, S3StorageAdapter)
    
    def test_create_azure_adapter(self):
        """Test creating Azure adapter."""
        config = {'container_name': 'test', 'account_name': 'account'}
        credentials = {'account_key': 'key'}
        
        adapter = get_storage_adapter('azure', config, credentials)
        assert isinstance(adapter, AzureStorageAdapter)
    
    def test_create_minio_adapter(self):
        """Test creating MinIO adapter."""
        config = {'bucket_name': 'test', 'endpoint_url': 'localhost:9000'}
        credentials = {'access_key': 'key', 'secret_key': 'secret'}
        
        adapter = get_storage_adapter('minio', config, credentials)
        assert isinstance(adapter, MinIOStorageAdapter)
    
    def test_create_local_adapter(self):
        """Test creating Local adapter."""
        config = {'base_path': '/tmp/storage'}
        
        adapter = get_storage_adapter('local', config)
        assert isinstance(adapter, LocalStorageAdapter)
    
    def test_case_insensitive_backend(self):
        """Test that backend name is case-insensitive."""
        config = {'base_path': '/tmp/storage'}
        
        adapter1 = get_storage_adapter('LOCAL', config)
        adapter2 = get_storage_adapter('local', config)
        adapter3 = get_storage_adapter('Local', config)
        
        assert type(adapter1) == type(adapter2) == type(adapter3)
    
    def test_unknown_backend(self):
        """Test that unknown backend raises error."""
        with pytest.raises(StorageAdapterError) as exc_info:
            get_storage_adapter('unknown', {})
        
        assert 'Unknown storage backend' in str(exc_info.value)
        assert 'unknown' in str(exc_info.value)
    
    def test_invalid_config_raises_error(self):
        """Test that invalid config raises error."""
        with pytest.raises(StorageAdapterError):
            # Missing required bucket_name
            get_storage_adapter('s3', {})
    
    def test_whitespace_in_backend_name(self):
        """Test that whitespace is stripped from backend name."""
        config = {'base_path': '/tmp/storage'}
        adapter = get_storage_adapter('  local  ', config)
        assert isinstance(adapter, LocalStorageAdapter)


class TestRegisterAdapter:
    """Test register_adapter function."""
    
    def test_register_custom_adapter(self):
        """Test registering a custom adapter."""
        class CustomAdapter(BaseStorageAdapter):
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
        
        register_adapter('custom', CustomAdapter)
        
        adapter = get_storage_adapter('custom', {})
        assert isinstance(adapter, CustomAdapter)
    
    def test_register_non_adapter_class(self):
        """Test that registering non-adapter class raises error."""
        class NotAnAdapter:
            pass
        
        with pytest.raises(ValueError) as exc_info:
            register_adapter('invalid', NotAnAdapter)
        
        assert 'must inherit from BaseStorageAdapter' in str(exc_info.value)
    
    def test_override_existing_adapter(self):
        """Test that registering can override existing adapter."""
        class CustomLocal(LocalStorageAdapter):
            pass
        
        register_adapter('local', CustomLocal)
        
        config = {'base_path': '/tmp/storage'}
        adapter = get_storage_adapter('local', config)
        assert isinstance(adapter, CustomLocal)
        
        # Reset to original
        register_adapter('local', LocalStorageAdapter)


class TestListAvailableBackends:
    """Test list_available_backends function."""
    
    def test_list_backends(self):
        """Test listing available backends."""
        backends = list_available_backends()
        
        assert isinstance(backends, list)
        assert 's3' in backends
        assert 'azure' in backends
        assert 'minio' in backends
        assert 'local' in backends
    
    def test_list_includes_custom_backends(self):
        """Test that list includes custom registered backends."""
        class CustomAdapter(BaseStorageAdapter):
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
        
        register_adapter('test_custom', CustomAdapter)
        backends = list_available_backends()
        
        assert 'test_custom' in backends


class TestGetAdapterInfo:
    """Test get_adapter_info function."""
    
    def test_get_s3_info(self):
        """Test getting S3 adapter info."""
        info = get_adapter_info('s3')
        
        assert info['backend'] == 's3'
        assert info['class_name'] == 'S3StorageAdapter'
        assert 'module' in info
        assert 'docstring' in info
    
    def test_get_azure_info(self):
        """Test getting Azure adapter info."""
        info = get_adapter_info('azure')
        
        assert info['backend'] == 'azure'
        assert info['class_name'] == 'AzureStorageAdapter'
    
    def test_unknown_backend_raises_error(self):
        """Test that unknown backend raises error."""
        with pytest.raises(StorageAdapterError) as exc_info:
            get_adapter_info('nonexistent')
        
        assert 'Unknown storage backend' in str(exc_info.value)