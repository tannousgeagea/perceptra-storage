"""
Amazon S3 storage adapter implementation.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, BinaryIO, Dict, Any
import logging

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError, ConnectionError
    from botocore.config import Config
except ImportError:
    raise ImportError(
        "boto3 is required for S3 adapter. Install with: pip install boto3"
    )

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


class S3StorageAdapter(BaseStorageAdapter):
    """
    Amazon S3 storage adapter.
    
    Configuration required:
        - bucket_name: S3 bucket name
        - region: AWS region (optional, defaults to us-east-1)
    
    Credentials required:
        - access_key_id: AWS access key ID
        - secret_access_key: AWS secret access key
        - session_token: AWS session token (optional, for temporary credentials)
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[Dict[str, Any]] = None):
        """Initialize S3 storage adapter."""
        super().__init__(config, credentials)
        self._client = None
        self._initialize_client()

    def _validate_config(self) -> None:
        """Validate S3 configuration."""
        if 'bucket_name' not in self.config:
            raise ValueError("S3 adapter requires 'bucket_name' in config")
        
        if not self.config['bucket_name']:
            raise ValueError("bucket_name cannot be empty")

    def _initialize_client(self) -> None:
        """Initialize boto3 S3 client."""
        try:
            region = self.config.get('region', 'us-east-1')
            
            # Configure boto3 with timeout and retry settings
            boto_config = Config(
                region_name=region,
                connect_timeout=10,
                read_timeout=30,
                retries={'max_attempts': 3, 'mode': 'standard'}
            )
            
            # Build session kwargs
            session_kwargs = {}
            if self.credentials:
                session_kwargs['aws_access_key_id'] = self.credentials.get('access_key_id')
                session_kwargs['aws_secret_access_key'] = self.credentials.get('secret_access_key')
                
                if 'session_token' in self.credentials:
                    session_kwargs['aws_session_token'] = self.credentials['session_token']
            
            self._client = boto3.client('s3', config=boto_config, **session_kwargs)
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise StorageConnectionError(f"S3 client initialization failed: {e}")

    def test_connection(self, timeout: int = 10) -> bool:
        """Test S3 connection by checking bucket access."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Try to head the bucket to verify access
            self._client.head_bucket(Bucket=bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            if error_code == '404':
                raise StorageConnectionError(f"S3 bucket '{bucket_name}' does not exist")
            elif error_code == '403':
                raise StoragePermissionError(f"Access denied to S3 bucket '{bucket_name}'")
            else:
                raise StorageConnectionError(f"S3 connection failed: {e}")
                
        except (BotoCoreError, ConnectionError) as e:
            raise StorageConnectionError(f"S3 connection failed: {e}")

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to S3."""
        try:
            bucket_name = self.config['bucket_name']
            extra_args = {}
            
            if content_type:
                extra_args['ContentType'] = content_type
            
            if metadata:
                extra_args['Metadata'] = metadata
            
            self._client.upload_fileobj(
                file_obj,
                bucket_name,
                key,
                ExtraArgs=extra_args if extra_args else None
            )
            
            logger.info(f"Uploaded file to S3: s3://{bucket_name}/{key}")
            return key
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '403':
                raise StoragePermissionError(f"Permission denied uploading to S3: {key}")
            raise StorageOperationError(f"S3 upload failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"S3 upload failed: {e}")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        """Download file from S3."""
        try:
            bucket_name = self.config['bucket_name']
            
            if destination:
                self._client.download_file(bucket_name, key, str(destination))
                logger.info(f"Downloaded S3 file to: {destination}")
                with open(destination, 'rb') as f:
                    return f.read()
            else:
                response = self._client.get_object(Bucket=bucket_name, Key=key)
                data = response['Body'].read()
                logger.info(f"Downloaded S3 file: s3://{bucket_name}/{key}")
                return data
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                raise StorageNotFoundError(f"File not found in S3: {key}")
            elif error_code == '403':
                raise StoragePermissionError(f"Permission denied downloading from S3: {key}")
            raise StorageOperationError(f"S3 download failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"S3 download failed: {e}")

    def delete_file(self, key: str) -> bool:
        """Delete file from S3."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Check if file exists first
            if not self.file_exists(key):
                raise StorageNotFoundError(f"File not found in S3: {key}")
            
            self._client.delete_object(Bucket=bucket_name, Key=key)
            logger.info(f"Deleted S3 file: s3://{bucket_name}/{key}")
            return True
            
        except StorageNotFoundError:
            raise
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '403':
                raise StoragePermissionError(f"Permission denied deleting from S3: {key}")
            raise StorageOperationError(f"S3 delete failed: {e}")
        except Exception as e:
            raise StorageOperationError(f"S3 delete failed: {e}")

    def file_exists(self, key: str) -> bool:
        """Check if file exists in S3."""
        try:
            bucket_name = self.config['bucket_name']
            self._client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                return False
            raise StorageOperationError(f"S3 file_exists check failed: {e}")

    def get_file_metadata(self, key: str) -> StorageObject:
        """Get file metadata from S3."""
        try:
            bucket_name = self.config['bucket_name']
            response = self._client.head_object(Bucket=bucket_name, Key=key)
            
            return StorageObject(
                key=key,
                size=response['ContentLength'],
                last_modified=response['LastModified'],
                etag=response.get('ETag', '').strip('"'),
                content_type=response.get('ContentType'),
                metadata=response.get('Metadata', {})
            )
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                raise StorageNotFoundError(f"File not found in S3: {key}")
            raise StorageOperationError(f"S3 metadata retrieval failed: {e}")

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        """List files in S3 bucket."""
        try:
            bucket_name = self.config['bucket_name']
            files = []
            
            paginator = self._client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                PaginationConfig={'MaxItems': max_results}
            )
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    files.append(StorageObject(
                        key=obj['Key'],
                        size=obj['Size'],
                        last_modified=obj['LastModified'],
                        etag=obj.get('ETag', '').strip('"')
                    ))
            
            logger.info(f"Listed {len(files)} files from S3 with prefix: {prefix}")
            return files
            
        except ClientError as e:
            raise StorageOperationError(f"S3 list operation failed: {e}")

    def generate_presigned_url(
        self,
        key: str,
        expiration: int = 3600,
        method: str = "GET"
    ) -> PresignedUrl:
        """Generate presigned URL for S3 object."""
        try:
            bucket_name = self.config['bucket_name']
            
            # Map method to S3 operation
            operation_map = {
                'GET': 'get_object',
                'PUT': 'put_object',
                'DELETE': 'delete_object'
            }
            
            operation = operation_map.get(method.upper())
            if not operation:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            url = self._client.generate_presigned_url(
                operation,
                Params={'Bucket': bucket_name, 'Key': key},
                ExpiresIn=expiration
            )
            
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expiration)
            
            return PresignedUrl(
                url=url,
                expires_at=expires_at,
                method=method.upper()
            )
            
        except ClientError as e:
            raise StorageOperationError(f"S3 presigned URL generation failed: {e}")

    def get_public_url(self, key: str) -> Optional[str]:
        """Get public URL for S3 object (if bucket is public)."""
        bucket_name = self.config['bucket_name']
        region = self.config.get('region', 'us-east-1')
        
        # Standard S3 URL format
        if region == 'us-east-1':
            return f"https://{bucket_name}.s3.amazonaws.com/{key}"
        else:
            return f"https://{bucket_name}.s3.{region}.amazonaws.com/{key}"