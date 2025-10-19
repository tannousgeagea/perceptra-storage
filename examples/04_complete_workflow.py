"""
Complete real-world workflow example for a Computer Vision platform.

This demonstrates:
- Multi-tenant dataset management
- Image upload and processing
- Model storage and versioning
- Result archiving
- Error handling and logging
"""
import logging
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetManager:
    """Manages datasets for computer vision projects."""
    
    def __init__(self, storage_adapter, tenant_id: str):
        self.storage = storage_adapter
        self.tenant_id = tenant_id
        self.base_path = f"tenants/{tenant_id}/datasets"
    
    def upload_images(self, dataset_name: str, images: List[Path]) -> Dict:
        """
        Upload multiple images for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            images: List of image file paths
        
        Returns:
            Dictionary with upload results
        """
        logger.info(f"Uploading {len(images)} images for dataset: {dataset_name}")
        
        results = {
            'uploaded': [],
            'failed': [],
            'total_size': 0
        }
        
        for img_path in images:
            try:
                # Generate storage key
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                key = f"{self.base_path}/{dataset_name}/images/{timestamp}_{img_path.name}"
                
                # Upload with metadata
                with open(img_path, 'rb') as f:
                    self.storage.upload_file(
                        f,
                        key,
                        content_type='image/jpeg',
                        metadata={
                            'dataset': dataset_name,
                            'original_name': img_path.name,
                            'uploaded_at': timestamp
                        }
                    )
                
                file_size = img_path.stat().st_size
                results['uploaded'].append({
                    'key': key,
                    'size': file_size,
                    'name': img_path.name
                })
                results['total_size'] += file_size
                
                logger.info(f"  ✓ Uploaded: {img_path.name}")
                
            except Exception as e:
                logger.error(f"  ✗ Failed: {img_path.name} - {e}")
                results['failed'].append({
                    'name': img_path.name,
                    'error': str(e)
                })
        
        logger.info(f"Upload complete: {len(results['uploaded'])} succeeded, {len(results['failed'])} failed")
        return results
    
    def create_annotations(self, dataset_name: str, annotations: Dict) -> str:
        """
        Upload annotation file for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            annotations: Annotation data (dict)
        
        Returns:
            Storage key of annotation file
        """
        key = f"{self.base_path}/{dataset_name}/annotations.json"
        
        # Convert to JSON bytes
        json_data = json.dumps(annotations, indent=2).encode()
        
        self.storage.upload_file(
            BytesIO(json_data),
            key,
            content_type='application/json'
        )
        
        logger.info(f"✓ Annotations uploaded: {key}")
        return key
    
    def list_datasets(self) -> List[str]:
        """List all datasets for this tenant."""
        files = self.storage.list_files(prefix=f"{self.base_path}/")
        
        # Extract unique dataset names
        datasets = set()
        for file_obj in files:
            parts = file_obj.key.split('/')
            if len(parts) >= 4:  # tenants/id/datasets/name/...
                datasets.add(parts[3])
        
        return sorted(datasets)


class ModelManager:
    """Manages trained models and their versions."""
    
    def __init__(self, storage_adapter, tenant_id: str):
        self.storage = storage_adapter
        self.tenant_id = tenant_id
        self.base_path = f"tenants/{tenant_id}/models"
    
    def upload_model(
        self,
        model_name: str,
        version: str,
        model_file: Path,
        metadata: Dict
    ) -> Dict:
        """
        Upload a trained model with version.
        
        Args:
            model_name: Name of the model
            version: Version string (e.g., 'v1.0.0')
            model_file: Path to model file
            metadata: Model metadata (accuracy, parameters, etc.)
        
        Returns:
            Dictionary with upload information
        """
        logger.info(f"Uploading model: {model_name} {version}")
        
        # Upload model weights
        key = f"{self.base_path}/{model_name}/{version}/model.pkl"
        
        with open(model_file, 'rb') as f:
            self.storage.upload_file(
                f,
                key,
                content_type='application/octet-stream',
                metadata={
                    'model_name': model_name,
                    'version': version,
                    'uploaded_at': datetime.now().isoformat()
                }
            )
        
        # Upload metadata
        metadata_key = f"{self.base_path}/{model_name}/{version}/metadata.json"
        metadata_json = json.dumps(metadata, indent=2).encode()
        
        self.storage.upload_file(
            BytesIO(metadata_json),
            metadata_key,
            content_type='application/json'
        )
        
        # Get presigned URL for download
        presigned = self.storage.generate_presigned_url(key, expiration=86400)
        
        result = {
            'model_key': key,
            'metadata_key': metadata_key,
            'version': version,
            'download_url': presigned.url,
            'size': model_file.stat().st_size
        }
        
        logger.info(f"✓ Model uploaded: {model_name} {version}")
        return result
    
    def get_latest_version(self, model_name: str) -> Optional[str]:
        """Get the latest version of a model."""
        files = self.storage.list_files(prefix=f"{self.base_path}/{model_name}/")
        
        versions = set()
        for file_obj in files:
            parts = file_obj.key.split('/')
            if len(parts) >= 5:  # tenants/id/models/name/version/...
                versions.add(parts[4])
        
        if not versions:
            return None
        
        # Sort versions (assumes semantic versioning)
        return sorted(versions)[-1]
    
    def download_model(self, model_name: str, version: str) -> bytes:
        """Download a specific model version."""
        key = f"{self.base_path}/{model_name}/{version}/model.pkl"
        
        logger.info(f"Downloading model: {model_name} {version}")
        return self.storage.download_file(key)


class InferenceResultManager:
    """Manages inference results and predictions."""
    
    def __init__(self, storage_adapter, tenant_id: str):
        self.storage = storage_adapter
        self.tenant_id = tenant_id
        self.base_path = f"tenants/{tenant_id}/results"
    
    def save_predictions(
        self,
        job_id: str,
        predictions: List[Dict]
    ) -> str:
        """
        Save inference predictions.
        
        Args:
            job_id: Unique job identifier
            predictions: List of prediction results
        
        Returns:
            Storage key of results file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"{self.base_path}/{timestamp}_{job_id}_predictions.json"
        
        results_data = {
            'job_id': job_id,
            'timestamp': timestamp,
            'predictions': predictions,
            'count': len(predictions)
        }
        
        json_data = json.dumps(results_data, indent=2).encode()
        
        self.storage.upload_file(
            BytesIO(json_data),
            key,
            content_type='application/json'
        )
        
        logger.info(f"✓ Predictions saved: {key}")
        return key
    
    def save_visualization(self, job_id: str, image_data: bytes) -> str:
        """Save visualization image."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"{self.base_path}/{timestamp}_{job_id}_visualization.jpg"
        
        self.storage.upload_file(
            BytesIO(image_data),
            key,
            content_type='image/jpeg'
        )
        
        logger.info(f"✓ Visualization saved: {key}")
        return key


def example_complete_workflow():
    """
    Complete workflow example: Upload dataset, train model, run inference.
    """
    print("\n" + "="*60)
    print("COMPLETE COMPUTER VISION WORKFLOW")
    print("="*60)
    
    from perceptra_storage import get_storage_adapter
    
    # Setup storage for tenant
    tenant_id = "demo-company-001"
    adapter = get_storage_adapter('local', {
        'base_path': f'/tmp/cv_platform/{tenant_id}',
        'create_dirs': True
    })
    
    # Initialize managers
    dataset_mgr = DatasetManager(adapter, tenant_id)
    model_mgr = ModelManager(adapter, tenant_id)
    result_mgr = InferenceResultManager(adapter, tenant_id)
    
    # ===== PHASE 1: Dataset Upload =====
    print("\n--- Phase 1: Dataset Upload ---")
    
    # Create dummy image files for demo
    demo_dir = Path('/tmp/demo_images')
    demo_dir.mkdir(parents=True, exist_ok=True)
    
    dummy_images = []
    for i in range(5):
        img_path = demo_dir / f"image_{i:03d}.jpg"
        img_path.write_bytes(b"dummy_image_data_" + str(i).encode())
        dummy_images.append(img_path)
    
    # Upload images
    upload_results = dataset_mgr.upload_images(
        dataset_name="object_detection_v1",
        images=dummy_images
    )
    
    print(f"✓ Uploaded {len(upload_results['uploaded'])} images")
    print(f"  Total size: {upload_results['total_size']} bytes")
    
    # Create annotations
    annotations = {
        'version': '1.0',
        'images': [
            {
                'id': i,
                'file_name': f'image_{i:03d}.jpg',
                'width': 640,
                'height': 480
            }
            for i in range(5)
        ],
        'annotations': [
            {
                'id': i,
                'image_id': i,
                'category_id': 1,
                'bbox': [100, 100, 200, 200],
                'area': 40000
            }
            for i in range(5)
        ],
        'categories': [
            {'id': 1, 'name': 'person'},
            {'id': 2, 'name': 'car'}
        ]
    }
    
    annotation_key = dataset_mgr.create_annotations(
        dataset_name="object_detection_v1",
        annotations=annotations
    )
    
    # List datasets
    datasets = dataset_mgr.list_datasets()
    print(f"✓ Available datasets: {datasets}")
    
    # ===== PHASE 2: Model Upload =====
    print("\n--- Phase 2: Model Upload ---")
    
    # Create dummy model file
    model_file = Path('/tmp/trained_model.pkl')
    model_file.write_bytes(b"trained_model_weights_data" * 100)
    
    model_metadata = {
        'architecture': 'YOLOv8',
        'dataset': 'object_detection_v1',
        'epochs': 100,
        'batch_size': 16,
        'learning_rate': 0.001,
        'metrics': {
            'accuracy': 0.95,
            'precision': 0.93,
            'recall': 0.92,
            'f1_score': 0.925
        },
        'training_time': '2h 30m',
        'parameters': 11000000
    }
    
    model_info = model_mgr.upload_model(
        model_name='object_detector',
        version='v1.0.0',
        model_file=model_file,
        metadata=model_metadata
    )
    
    print(f"✓ Model uploaded:")
    print(f"  Key: {model_info['model_key']}")
    print(f"  Size: {model_info['size']} bytes")
    print(f"  Version: {model_info['version']}")
    
    # Get latest version
    latest = model_mgr.get_latest_version('object_detector')
    print(f"✓ Latest version: {latest}")
    
    # ===== PHASE 3: Inference Results =====
    print("\n--- Phase 3: Save Inference Results ---")
    
    # Simulate inference results
    predictions = [
        {
            'image_id': 0,
            'detections': [
                {
                    'class': 'person',
                    'confidence': 0.95,
                    'bbox': [100, 100, 200, 200]
                },
                {
                    'class': 'car',
                    'confidence': 0.87,
                    'bbox': [300, 150, 400, 250]
                }
            ]
        },
        {
            'image_id': 1,
            'detections': [
                {
                    'class': 'person',
                    'confidence': 0.92,
                    'bbox': [50, 80, 150, 180]
                }
            ]
        }
    ]
    
    job_id = 'inference_20250118_001'
    
    # Save predictions
    pred_key = result_mgr.save_predictions(job_id, predictions)
    print(f"✓ Predictions saved: {pred_key}")
    
    # Save visualization
    viz_data = b"visualization_image_data" * 50
    viz_key = result_mgr.save_visualization(job_id, viz_data)
    print(f"✓ Visualization saved: {viz_key}")
    
    # ===== PHASE 4: Retrieval and Summary =====
    print("\n--- Phase 4: Storage Summary ---")
    
    all_files = adapter.list_files(prefix=f"tenants/{tenant_id}/")
    
    # Categorize files
    categories = {
        'datasets': [],
        'models': [],
        'results': []
    }
    
    total_size = 0
    for file_obj in all_files:
        total_size += file_obj.size
        
        if 'datasets' in file_obj.key:
            categories['datasets'].append(file_obj)
        elif 'models' in file_obj.key:
            categories['models'].append(file_obj)
        elif 'results' in file_obj.key:
            categories['results'].append(file_obj)
    
    print(f"\n✓ Storage Summary:")
    print(f"  Total files: {len(all_files)}")
    print(f"  Total size: {total_size:,} bytes")
    print(f"  Dataset files: {len(categories['datasets'])}")
    print(f"  Model files: {len(categories['models'])}")
    print(f"  Result files: {len(categories['results'])}")
    
    # Generate download URLs
    print("\n--- Download URLs ---")
    for file_obj in categories['models'][:3]:
        presigned = adapter.generate_presigned_url(
            file_obj.key,
            expiration=3600
        )
        print(f"  {file_obj.key}")
        print(f"  → {presigned.url[:80]}...")
    
    print("\n" + "="*60)
    print("Workflow completed successfully!")
    print("="*60)


def example_with_django():
    """
    Example showing Django integration with the workflow.
    
    Note: This would be run in Django shell or management command.
    """
    print("\n=== DJANGO INTEGRATION EXAMPLE ===")
    print("Run this in Django shell:")
    print()
    print("```python")
    print("from core.models import Tenant")
    print("from storage.models import StorageProfile")
    print("from storage.services import get_storage_adapter_for_profile")
    print()
    print("# Get tenant and storage profile")
    print("tenant = Tenant.objects.get(name='demo-company')")
    print("profile = StorageProfile.objects.get(tenant=tenant, is_default=True)")
    print()
    print("# Get adapter")
    print("adapter = get_storage_adapter_for_profile(profile)")
    print()
    print("# Initialize managers")
    print("dataset_mgr = DatasetManager(adapter, str(tenant.id))")
    print("model_mgr = ModelManager(adapter, str(tenant.id))")
    print("result_mgr = InferenceResultManager(adapter, str(tenant.id))")
    print()
    print("# Use managers as shown in complete workflow")
    print("# ...")
    print("```")


def example_error_recovery():
    """
    Example showing error handling and recovery strategies.
    """
    print("\n=== ERROR HANDLING & RECOVERY ===")
    
    from perceptra_storage import (
        get_storage_adapter,
        StorageError,
        StorageNotFoundError,
        StorageConnectionError
    )
    
    adapter = get_storage_adapter('local', {
        'base_path': '/tmp/error_demo',
        'create_dirs': True
    })
    
    class RobustUploader:
        """Upload manager with retry logic and error recovery."""
        
        def __init__(self, storage_adapter, max_retries=3):
            self.storage = storage_adapter
            self.max_retries = max_retries
        
        def upload_with_retry(self, file_obj, key: str) -> Dict:
            """Upload with automatic retry on failure."""
            for attempt in range(self.max_retries):
                try:
                    self.storage.upload_file(file_obj, key)
                    return {
                        'success': True,
                        'key': key,
                        'attempts': attempt + 1
                    }
                except StorageConnectionError as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(f"Connection failed, retrying... ({attempt + 1}/{self.max_retries})")
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    else:
                        logger.error(f"Upload failed after {self.max_retries} attempts")
                        return {
                            'success': False,
                            'error': str(e),
                            'attempts': self.max_retries
                        }
                except StorageError as e:
                    logger.error(f"Storage error: {e}")
                    return {
                        'success': False,
                        'error': str(e),
                        'attempts': attempt + 1
                    }
            
            return {'success': False, 'error': 'Unknown error'}
        
        def batch_upload_with_recovery(self, files: Dict[str, bytes]) -> Dict:
            """Batch upload with failure recovery."""
            results = {
                'successful': [],
                'failed': [],
                'total': len(files)
            }
            
            for key, content in files.items():
                result = self.upload_with_retry(BytesIO(content), key)
                
                if result['success']:
                    results['successful'].append(key)
                    logger.info(f"✓ Uploaded: {key} (attempts: {result['attempts']})")
                else:
                    results['failed'].append({
                        'key': key,
                        'error': result['error']
                    })
                    logger.error(f"✗ Failed: {key} - {result['error']}")
            
            return results
    
    # Demo robust uploading
    uploader = RobustUploader(adapter)
    
    files = {
        f'test_{i}.txt': f'Content {i}'.encode()
        for i in range(10)
    }
    
    print("\nUploading with error recovery...")
    results = uploader.batch_upload_with_recovery(files)
    
    print(f"\n✓ Upload Results:")
    print(f"  Successful: {len(results['successful'])}/{results['total']}")
    print(f"  Failed: {len(results['failed'])}/{results['total']}")


def main():
    """Run all complete workflow examples."""
    print("="*60)
    print("PERCEPTRA STORAGE - COMPLETE WORKFLOW EXAMPLES")
    print("="*60)
    
    try:
        example_complete_workflow()
    except Exception as e:
        print(f"✗ Workflow error: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        example_with_django()
    except Exception as e:
        print(f"✗ Django example error: {e}")
    
    try:
        example_error_recovery()
    except Exception as e:
        print(f"✗ Error recovery example error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60)
    print("All examples completed!")
    print("="*60)


if __name__ == '__main__':
    main()