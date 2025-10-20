"""
Basic usage examples for perceptra-storage.

This example demonstrates the fundamental operations:
- Creating storage adapters
- Uploading and downloading files
- Testing connections
- Basic file operations
"""
from io import BytesIO
from perceptra_storage import get_storage_adapter


def example_local_storage():
    """Example: Local filesystem storage."""
    print("\n=== LOCAL FILESYSTEM EXAMPLE ===")
    
    config = {
        'base_path': '/tmp/perceptra_storage_example',
        'create_dirs': True
    }
    
    # Create adapter
    adapter = get_storage_adapter('local', config)
    
    # Test connection
    if adapter.test_connection():
        print("✓ Connection successful")
    
    # Upload a file
    content = b"Hello, Perceptra Storage!"
    file_obj = BytesIO(content)
    key = adapter.upload_file(file_obj, 'hello.txt')
    print(f"✓ Uploaded file: {key}")
    
    # Check if file exists
    if adapter.file_exists('hello.txt'):
        print("✓ File exists")
    
    # Get file metadata
    metadata = adapter.get_file_metadata('hello.txt')
    print(f"✓ File size: {metadata.size} bytes")
    print(f"✓ Last modified: {metadata.last_modified}")
    
    # Download the file
    downloaded = adapter.download_file('hello.txt')
    print(f"✓ Downloaded content: {downloaded.decode()}")
    
    # List files
    files = adapter.list_files()
    print(f"✓ Total files: {len(files)}")
    
    # Delete the file
    adapter.delete_file('hello.txt')
    print("✓ File deleted")


def example_s3_storage():
    """Example: Amazon S3 storage."""
    print("\n=== AMAZON S3 EXAMPLE ===")
    
    config = {
        'bucket_name': 'my-perceptra-bucket',
        'region': 'us-west-2'
    }
    
    credentials = {
        'access_key_id': 'YOUR_ACCESS_KEY_ID',
        'secret_access_key': 'YOUR_SECRET_ACCESS_KEY'
    }
    
    try:
        adapter = get_storage_adapter('s3', config, credentials)
        
        # Test connection
        adapter.test_connection()
        print("✓ Connected to S3")
        
        # Upload with metadata
        data = BytesIO(b"Dataset for computer vision model")
        key = adapter.upload_file(
            data,
            'datasets/2025/training_data.csv',
            content_type='text/csv',
            metadata={
                'project': 'object-detection',
                'version': '1.0'
            }
        )
        print(f"✓ Uploaded to S3: {key}")
        
        # Generate presigned URL (for sharing)
        presigned = adapter.generate_presigned_url(
            'datasets/2025/training_data.csv',
            expiration=3600  # 1 hour
        )
        print(f"✓ Presigned URL: {presigned.url[:50]}...")
        print(f"✓ Expires at: {presigned.expires_at}")
        
        # Get public URL
        public_url = adapter.get_public_url('datasets/2025/training_data.csv')
        print(f"✓ Public URL: {public_url}")
        
    except Exception as e:
        print(f"✗ S3 Example failed: {e}")
        print("Note: Replace with your actual AWS credentials")


def example_azure_storage():
    import os
    
    """Example: Azure Blob Storage."""
    print("\n=== AZURE BLOB STORAGE EXAMPLE ===")
    
    config = {
        'container_name': os.getenv("AZURE_CONTAINER_NAME"),
        'account_name': os.getenv("AZURE_ACCOUNT_NAME")
    }
    
    credentials = {
        'account_key': os.getenv("AZURE_ACCOUNT_KEY")
        # Or use connection string:
        # 'connection_string': 'DefaultEndpointsProtocol=https;AccountName=...'
    }
    
    try:
        adapter = get_storage_adapter('azure', config, credentials)
        
        adapter.test_connection()
        print("✓ Connected to Azure")
        
        # Upload model file
        model_data = BytesIO(b"Pretrained model weights...")
        adapter.upload_file(
            model_data,
            'models/yolov8/weights.pt',
            content_type='application/octet-stream'
        )
        print("✓ Uploaded model to Azure")
        
        # List files
        files = adapter.list_files()
        print(f"✓ Total files: {len(files)}")
        
        # Delete the file
        adapter.delete_file('models/yolov8/weights.pt')
        print("✓ File deleted")
        
    except Exception as e:
        print(f"✗ Azure Example failed: {e}")
        print("Note: Replace with your actual Azure credentials")


def example_minio_storage():
    """Example: MinIO / S3-compatible storage."""
    print("\n=== MINIO EXAMPLE ===")
    
    config = {
        'bucket_name': 'datasets',
        'endpoint_url': 'play.min.io:9000',  # Public MinIO playground
        'secure': True,
        'region': 'us-east-1'
    }
    
    credentials = {
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin'
    }
    
    try:
        adapter = get_storage_adapter('minio', config, credentials)
        
        adapter.test_connection()
        print("✓ Connected to MinIO")
        
        # Upload image
        image_data = BytesIO(b"Binary image data...")
        adapter.upload_file(
            image_data,
            'images/sample.jpg',
            content_type='image/jpeg'
        )
        print("✓ Uploaded image to MinIO")
        
    except Exception as e:
        print(f"✗ MinIO Example failed: {e}")


def example_file_operations():
    """Example: Common file operations across all backends."""
    print("\n=== FILE OPERATIONS EXAMPLE ===")
    
    # Using local storage for demo
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/file_ops_demo', 'create_dirs': True}
    )
    
    # Create multiple files
    files = {
        'documents/report.pdf': b'PDF content',
        'documents/summary.txt': b'Summary text',
        'images/photo1.jpg': b'Image data 1',
        'images/photo2.jpg': b'Image data 2',
        'data/dataset.csv': b'CSV data'
    }
    
    print("Uploading files...")
    for key, content in files.items():
        adapter.upload_file(BytesIO(content), key)
        print(f"  ✓ {key}")
    
    # List all files
    print("\nListing all files:")
    all_files = adapter.list_files()
    for f in all_files:
        print(f"  - {f.key} ({f.size} bytes)")
    
    # List files with prefix
    print("\nListing images only:")
    image_files = adapter.list_files(prefix='images/')
    for f in image_files:
        print(f"  - {f.key}")
    
    # Check file existence
    print("\nChecking file existence:")
    print(f"  documents/report.pdf exists: {adapter.file_exists('documents/report.pdf')}")
    print(f"  nonexistent.txt exists: {adapter.file_exists('nonexistent.txt')}")
    
    # Cleanup
    print("\nCleaning up...")
    for key in files.keys():
        adapter.delete_file(key)
    print("  ✓ All files deleted")


def example_error_handling():
    """Example: Proper error handling."""
    print("\n=== ERROR HANDLING EXAMPLE ===")
    
    from perceptra_storage import (
        StorageNotFoundError,
        StorageConnectionError,
        StorageError
    )
    
    adapter = get_storage_adapter(
        'local',
        {'base_path': '/tmp/error_demo', 'create_dirs': True}
    )
    
    # Handle file not found
    try:
        adapter.download_file('nonexistent.txt')
    except StorageNotFoundError as e:
        print(f"✓ Caught expected error: {e}")
    
    # Handle general storage errors
    try:
        # Try to create adapter with invalid config
        bad_adapter = get_storage_adapter('s3', {})  # Missing bucket_name
    except StorageError as e:
        print(f"✓ Caught configuration error: {e}")
    
    print("✓ Error handling works correctly")


def main():
    """Run all basic examples."""
    print("=" * 60)
    print("PERCEPTRA STORAGE - BASIC USAGE EXAMPLES")
    print("=" * 60)
    
    # Run examples
    example_local_storage()
    example_file_operations()
    # example_error_handling()
    
    # Cloud examples (will fail without credentials)
    # example_s3_storage()
    example_azure_storage()
    # example_minio_storage()
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()