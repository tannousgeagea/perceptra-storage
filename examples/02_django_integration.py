"""
Django integration examples for perceptra-storage.

This demonstrates how to use the storage system within a Django application.
Run these examples in Django shell: python manage.py shell
"""

# Example 1: Creating Storage Profiles
def example_create_profiles():
    """Create storage profiles for a tenant."""
    from tenants.models import Tenant
    from storage.models import StorageProfile, SecretRef
    
    # Get or create tenant
    tenant, _ = Tenant.objects.get_or_create(
        name='demo-company',
        defaults={'slug': 'demo-company'}
    )
    
    # Create S3 profile with secret reference
    secret_ref = SecretRef.objects.create(
        tenant=tenant,
        provider='vault',
        path='secret/data/storage/s3-credentials',
        key='aws_credentials',
        metadata={'environment': 'production', 'region': 'us-west-2'}
    )
    
    s3_profile = StorageProfile.objects.create(
        tenant=tenant,
        name='Production S3 Storage',
        backend='s3',
        region='us-west-2',
        is_default=True,
        config={
            'bucket_name': 'demo-company-datasets',
            'region': 'us-west-2'
        },
        credential_ref=secret_ref,
        is_active=True
    )
    
    print(f"✓ Created S3 profile: {s3_profile}")
    
    # Create Azure profile
    azure_profile = StorageProfile.objects.create(
        tenant=tenant,
        name='Azure Model Storage',
        backend='azure',
        config={
            'container_name': 'ml-models',
            'account_name': 'democompanystorage'
        },
        credential_ref=SecretRef.objects.create(
            tenant=tenant,
            provider='azure_kv',
            path='storage-credentials',
            key='azure_storage'
        )
    )
    
    print(f"✓ Created Azure profile: {azure_profile}")
    
    # Create local profile for development
    local_profile = StorageProfile.objects.create(
        tenant=tenant,
        name='Local Development',
        backend='local',
        config={
            'base_path': f'/var/perceptra/storage/{tenant.slug}',
            'create_dirs': True
        }
    )
    
    print(f"✓ Created local profile: {local_profile}")
    
    return tenant, s3_profile, azure_profile, local_profile


# Example 2: Using Storage Profiles
def example_use_storage_profile():
    """Use a storage profile to upload/download files."""
    from storage.models import StorageProfile
    from storage.services import get_storage_adapter_for_profile
    from io import BytesIO
    
    # Get profile
    profile = StorageProfile.objects.filter(
        backend='local',
        is_active=True
    ).first()
    
    if not profile:
        print("✗ No active local storage profile found")
        return
    
    # Get adapter
    adapter = get_storage_adapter_for_profile(profile, test_connection=True)
    print(f"✓ Connected to: {profile.name}")
    
    # Upload dataset
    dataset_content = b"image_id,label,confidence\n1,cat,0.95\n2,dog,0.87"
    adapter.upload_file(
        BytesIO(dataset_content),
        'datasets/annotations.csv',
        content_type='text/csv',
        metadata={'created_by': 'example_script', 'version': '1.0'}
    )
    print("✓ Uploaded dataset")
    
    # Download and verify
    downloaded = adapter.download_file('datasets/annotations.csv')
    print(f"✓ Downloaded {len(downloaded)} bytes")
    
    # List files
    files = adapter.list_files(prefix='datasets/')
    print(f"✓ Found {len(files)} files in datasets/")
    
    return adapter


# Example 3: Testing Storage Connections
def example_test_connections():
    """Test all storage profiles for a tenant."""
    from storage.models import StorageProfile
    from storage.services import test_storage_profile_connection
    
    profiles = StorageProfile.objects.filter(is_active=True)
    
    print(f"\nTesting {profiles.count()} storage profiles:")
    print("-" * 60)
    
    for profile in profiles:
        success, error = test_storage_profile_connection(profile)
        
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status} | {profile.name} ({profile.backend})")
        
        if not success:
            print(f"       Error: {error}")
    
    print("-" * 60)


# Example 4: Using Context Manager
def example_context_manager():
    """Use StorageManager context manager for clean resource handling."""
    from storage.models import StorageProfile
    from storage.services import StorageManager
    from io import BytesIO
    
    profile = StorageProfile.objects.filter(backend='local').first()
    
    if not profile:
        print("✗ No local storage profile found")
        return
    
    # Use context manager
    with StorageManager(profile, test_connection=True) as storage:
        # Upload multiple files
        files_to_upload = {
            'reports/monthly_report.pdf': b'PDF content here',
            'reports/summary.txt': b'Executive summary',
            'data/metrics.json': b'{"accuracy": 0.95}'
        }
        
        for key, content in files_to_upload.items():
            storage.upload_file(BytesIO(content), key)
            print(f"✓ Uploaded: {key}")
        
        # List uploaded files
        files = storage.list_files(prefix='reports/')
        print(f"\n✓ Uploaded {len(files)} reports")
    
    # Context automatically handles cleanup
    print("✓ Context manager cleaned up resources")


# Example 5: Multi-Tenant File Management
def example_multi_tenant():
    """Manage files for multiple tenants."""
    from tenants.models import Tenant
    from storage.models import StorageProfile
    from storage.services import get_default_storage_adapter
    from io import BytesIO
    
    # Create multiple tenants
    tenants = []
    for name in ['acme-corp', 'tech-startup', 'enterprise-co']:
        tenant, _ = Tenant.objects.get_or_create(
            name=name,
            defaults={'slug': name}
        )
        tenants.append(tenant)
        
        # Create default local storage for each
        StorageProfile.objects.get_or_create(
            tenant=tenant,
            name='Default Local',
            defaults={
                'backend': 'local',
                'is_default': True,
                'config': {
                    'base_path': f'/var/perceptra/storage/{name}',
                    'create_dirs': True
                }
            }
        )
    
    # Upload tenant-specific data
    for tenant in tenants:
        adapter = get_default_storage_adapter(tenant)
        
        if adapter:
            # Each tenant's data is isolated
            data = f"Data for {tenant.name}".encode()
            adapter.upload_file(
                BytesIO(data),
                f'{tenant.slug}/config.txt'
            )
            print(f"✓ Uploaded data for: {tenant.name}")


# Example 6: Model Integration
def example_model_with_storage():
    """Integrate storage with Django models."""
    from django.db import models
    from storage.models import StorageProfile
    from storage.services import get_storage_adapter_for_profile
    
    # Example model that uses storage
    class Dataset(models.Model):
        """Dataset model with storage integration."""
        
        name = models.CharField(max_length=200)
        storage_profile = models.ForeignKey(
            StorageProfile,
            on_delete=models.PROTECT
        )
        storage_key = models.CharField(max_length=500)
        created_at = models.DateTimeField(auto_now_add=True)
        
        class Meta:
            app_label = 'examples'
        
        def upload_file(self, file_obj, filename):
            """Upload file to configured storage."""
            adapter = get_storage_adapter_for_profile(self.storage_profile)
            
            # Generate key with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            key = f'datasets/{self.name}/{timestamp}_{filename}'
            
            adapter.upload_file(file_obj, key)
            self.storage_key = key
            self.save()
            
            return key
        
        def download_file(self):
            """Download file from storage."""
            adapter = get_storage_adapter_for_profile(self.storage_profile)
            return adapter.download_file(self.storage_key)
        
        def get_presigned_url(self, expiration=3600):
            """Generate presigned URL for download."""
            adapter = get_storage_adapter_for_profile(self.storage_profile)
            return adapter.generate_presigned_url(
                self.storage_key,
                expiration=expiration
            )
    
    print("✓ Example Dataset model with storage integration")


# Example 7: Validation Before Save
def example_validate_before_save():
    """Validate storage configuration before saving profile."""
    from storage.models import StorageProfile
    from storage.services import test_storage_profile_connection
    from tenants.models import Tenant
    
    tenant = Tenant.objects.first()
    
    if not tenant:
        print("✗ No tenant found")
        return
    
    # Create profile but don't save yet
    new_profile = StorageProfile(
        tenant=tenant,
        name='New S3 Bucket',
        backend='s3',
        region='eu-west-1',
        config={
            'bucket_name': 'test-validation-bucket',
            'region': 'eu-west-1'
        }
    )
    
    # Test connection before saving
    print("Testing connection before saving...")
    success, error = test_storage_profile_connection(new_profile)
    
    if success:
        new_profile.save()
        print(f"✓ Profile validated and saved: {new_profile.name}")
    else:
        print(f"✗ Validation failed: {error}")
        print("Profile not saved")


# Example 8: Querying and Filtering Profiles
def example_query_profiles():
    """Query and filter storage profiles."""
    from storage.models import StorageProfile, StorageBackend
    from django.db.models import Count, Q
    
    # Get all active S3 profiles
    s3_profiles = StorageProfile.objects.filter(
        backend=StorageBackend.S3,
        is_active=True
    )
    print(f"Active S3 profiles: {s3_profiles.count()}")
    
    # Get default profile for each tenant
    from tenants.models import Tenant
    for tenant in Tenant.objects.all():
        default = StorageProfile.objects.filter(
            tenant=tenant,
            is_default=True
        ).first()
        
        if default:
            print(f"✓ {tenant.name}: {default.name} ({default.backend})")
    
    # Get profiles by backend type
    backend_counts = StorageProfile.objects.values('backend').annotate(
        count=Count('id')
    )
    
    print("\nProfiles by backend:")
    for item in backend_counts:
        print(f"  {item['backend']}: {item['count']}")
    
    # Get profiles that need credentials
    profiles_with_creds = StorageProfile.objects.filter(
        credential_ref__isnull=False
    ).select_related('credential_ref')
    
    print(f"\nProfiles with credentials: {profiles_with_creds.count()}")


# Example 9: Bulk Operations
def example_bulk_operations():
    """Perform bulk file operations."""
    from storage.models import StorageProfile
    from storage.services import get_storage_adapter_for_profile
    from io import BytesIO
    import time
    
    profile = StorageProfile.objects.filter(backend='local').first()
    
    if not profile:
        print("✗ No local storage profile found")
        return
    
    adapter = get_storage_adapter_for_profile(profile)
    
    # Bulk upload
    print("Bulk uploading 10 files...")
    start_time = time.time()
    
    for i in range(10):
        content = f"File content {i}".encode()
        adapter.upload_file(
            BytesIO(content),
            f'bulk_test/file_{i:03d}.txt'
        )
    
    elapsed = time.time() - start_time
    print(f"✓ Uploaded 10 files in {elapsed:.2f}s")
    
    # Bulk list
    files = adapter.list_files(prefix='bulk_test/')
    print(f"✓ Listed {len(files)} files")
    
    # Bulk delete
    print("Bulk deleting files...")
    for file_obj in files:
        adapter.delete_file(file_obj.key)
    
    print("✓ Deleted all test files")


# Example 10: Error Handling in Django
def example_django_error_handling():
    """Proper error handling in Django views."""
    from storage.models import StorageProfile
    from storage.services import (
        get_storage_adapter_for_profile,
        StorageServiceError
    )
    from perceptra_storage import (
        StorageNotFoundError,
        StoragePermissionError,
        StorageConnectionError
    )
    from django.http import JsonResponse
    import logging
    
    logger = logging.getLogger(__name__)
    
    def upload_file_view(request, profile_id):
        """Example view with proper error handling."""
        try:
            # Get profile
            profile = StorageProfile.objects.get(
                id=profile_id,
                tenant=request.user.tenant,
                is_active=True
            )
            
            # Get adapter
            adapter = get_storage_adapter_for_profile(profile)
            
            # Upload file
            file_obj = request.FILES.get('file')
            if not file_obj:
                return JsonResponse(
                    {'error': 'No file provided'},
                    status=400
                )
            
            key = f'uploads/{request.user.id}/{file_obj.name}'
            adapter.upload_file(file_obj, key)
            
            return JsonResponse({
                'status': 'success',
                'key': key,
                'size': file_obj.size
            })
            
        except StorageProfile.DoesNotExist:
            return JsonResponse(
                {'error': 'Storage profile not found'},
                status=404
            )
            
        except StorageConnectionError as e:
            logger.error(f"Storage connection failed: {e}")
            return JsonResponse(
                {'error': 'Could not connect to storage'},
                status=503
            )
            
        except StoragePermissionError as e:
            logger.error(f"Storage permission denied: {e}")
            return JsonResponse(
                {'error': 'Permission denied'},
                status=403
            )
            
        except StorageServiceError as e:
            logger.error(f"Storage service error: {e}")
            return JsonResponse(
                {'error': 'Storage operation failed'},
                status=500
            )
            
        except Exception as e:
            logger.exception("Unexpected error in upload")
            return JsonResponse(
                {'error': 'Internal server error'},
                status=500
            )
    
    print("✓ Example error handling view defined")


# Run all examples
def run_all_examples():
    """Run all Django integration examples."""
    print("=" * 60)
    print("PERCEPTRA STORAGE - DJANGO INTEGRATION EXAMPLES")
    print("=" * 60)
    
    print("\n1. Creating Storage Profiles")
    print("-" * 60)
    try:
        example_create_profiles()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n2. Using Storage Profiles")
    print("-" * 60)
    try:
        example_use_storage_profile()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n3. Testing Connections")
    print("-" * 60)
    try:
        example_test_connections()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n4. Context Manager Usage")
    print("-" * 60)
    try:
        example_context_manager()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n5. Multi-Tenant Management")
    print("-" * 60)
    try:
        example_multi_tenant()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n6. Model Integration")
    print("-" * 60)
    try:
        example_model_with_storage()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n7. Validation Before Save")
    print("-" * 60)
    try:
        example_validate_before_save()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n8. Querying Profiles")
    print("-" * 60)
    try:
        example_query_profiles()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n9. Bulk Operations")
    print("-" * 60)
    try:
        example_bulk_operations()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n10. Error Handling")
    print("-" * 60)
    try:
        example_django_error_handling()
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


# Usage instructions
"""
To run these examples in Django shell:

    python manage.py shell
    
    >>> from examples.django_integration import *
    >>> run_all_examples()
    
Or run individual examples:

    >>> example_create_profiles()
    >>> example_use_storage_profile()
    >>> example_test_connections()
"""