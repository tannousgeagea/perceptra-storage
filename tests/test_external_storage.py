"""
Tests for external (read-only) storage support: the read-only wrapper,
presign capabilities, Azure service-principal / SAS handling, S3-compatible
endpoints, MinIO credential names and the GCS adapter.
"""
import json
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from perceptra_storage import (
    LocalStorageAdapter,
    ReadOnlyStorageAdapter,
    StorageOperationError,
    StoragePermissionError,
    get_storage_adapter,
    list_available_backends,
)


@pytest.fixture
def temp_dir():
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


class TestReadOnlyWrapper:
    def test_reads_are_delegated_writes_refused(self, temp_dir):
        LocalStorageAdapter({'base_path': temp_dir}).upload_file(BytesIO(b"abc"), "a/b.jpg")
        adapter = get_storage_adapter('local', {'base_path': temp_dir}, read_only=True)

        assert isinstance(adapter, ReadOnlyStorageAdapter)
        assert adapter.read_only
        assert adapter.file_exists("a/b.jpg")
        assert adapter.download_file("a/b.jpg") == b"abc"
        assert [o.key for o in adapter.list_files("a/")] == ["a/b.jpg"]
        with pytest.raises(StoragePermissionError):
            adapter.upload_file(BytesIO(b"x"), "a/c.jpg")
        with pytest.raises(StoragePermissionError):
            adapter.delete_file("a/b.jpg")
        with pytest.raises(StoragePermissionError):
            adapter.generate_presigned_url("a/b.jpg", method="PUT")
        assert adapter.download_file("a/b.jpg") == b"abc", "nothing was deleted"

    def test_managed_adapter_is_not_read_only(self, temp_dir):
        adapter = get_storage_adapter('local', {'base_path': temp_dir})
        assert not adapter.read_only and adapter.can_presign


@pytest.mark.s3
class TestS3:
    def test_presign_and_endpoint_url(self):
        boto3 = pytest.importorskip("boto3")
        moto = pytest.importorskip("moto")
        with moto.mock_aws():
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="bkt-test")
            creds = {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "TOKEN"}
            adapter = get_storage_adapter("s3", {"bucket_name": "bkt-test"}, creds, read_only=True)
            url = adapter.generate_presigned_url("x/y.jpg", expiration=120).url
            assert "X-Amz-Expires=120" in url or "Expires=" in url
            assert "TOKEN" in url, "the session token is part of the signature"

        custom = get_storage_adapter(
            "s3", {"bucket_name": "b", "endpoint_url": "https://s3.example.com"}, {"access_key_id": "a", "secret_access_key": "b"},
        )
        assert custom._client.meta.endpoint_url == "https://s3.example.com"
        assert custom.get_public_url("k.jpg") == "https://s3.example.com/b/k.jpg"


@pytest.mark.minio
class TestMinIO:
    def test_accepts_s3_style_credential_names(self):
        pytest.importorskip("minio")
        adapter = get_storage_adapter(
            "minio", {"bucket_name": "b", "endpoint_url": "http://localhost:9000", "secure": False},
            {"access_key_id": "AK", "secret_access_key": "SK"},
        )
        creds = adapter._client._provider.retrieve()
        assert (creds.access_key, creds.secret_key) == ("AK", "SK")


@pytest.mark.azure
class TestAzure:
    CONFIG = {"account_name": "acct", "container_name": "c"}

    def test_capabilities(self):
        pytest.importorskip("azure.storage.blob")
        sas = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=rl&sig=x"})
        key = get_storage_adapter("azure", self.CONFIG, {"account_key": "a2V5"})
        sp = get_storage_adapter("azure", self.CONFIG, {"tenant_id": "t", "client_id": "c", "client_secret": "s"})
        assert (sas.can_presign, key.can_presign, sp.can_presign) == (False, True, True)

    def test_sas_token_is_never_handed_out_as_a_presigned_url(self):
        pytest.importorskip("azure.storage.blob")
        adapter = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=rl&sig=secret"})
        with pytest.raises(StorageOperationError):
            adapter.generate_presigned_url("a.jpg")

    def test_account_key_presign_is_short_lived(self):
        pytest.importorskip("azure.storage.blob")
        adapter = get_storage_adapter("azure", self.CONFIG, {"account_key": "a2V5a2V5a2V5"})
        url = adapter.generate_presigned_url("dir/a b.jpg", expiration=300)
        parsed = urlparse(url.url)
        assert parsed.path == "/c/dir/a%20b.jpg"
        query = parse_qs(parsed.query)
        assert query["sp"] == ["r"]
        expiry = datetime.fromisoformat(query["se"][0].replace("Z", "+00:00"))
        assert expiry - datetime.now(timezone.utc) < timedelta(seconds=310)

    def test_service_principal_uses_a_cached_user_delegation_key(self):
        blob = pytest.importorskip("azure.storage.blob")
        pytest.importorskip("azure.identity")
        adapter = get_storage_adapter(
            "azure", {**self.CONFIG, "endpoint_suffix": "core.example.net"},
            {"tenant_id": "t", "client_id": "c", "client_secret": "s"},
        )
        key = blob.UserDelegationKey()
        key.signed_oid, key.signed_tid, key.signed_service, key.signed_version = "oid", "tid", "b", "2021-08-06"
        key.signed_start, key.signed_expiry = "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"
        key.value = "a2V5a2V5a2V5"
        with patch.object(adapter._blob_service_client, "get_user_delegation_key", return_value=key) as get_key:
            first = adapter.generate_presigned_url("a.jpg", expiration=600)
            adapter.generate_presigned_url("b.jpg", expiration=600)
        assert get_key.call_count == 1
        assert first.url.startswith("https://acct.blob.core.example.net/c/a.jpg?")
        assert "skoid=oid" in first.url

    def test_connection_falls_back_to_list_for_narrow_credentials(self):
        pytest.importorskip("azure.storage.blob")
        from azure.core.exceptions import HttpResponseError

        adapter = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=rl&sig=x"})
        denied = HttpResponseError(message="denied")
        denied.status_code = 403
        container = MagicMock()
        container.get_container_properties.side_effect = denied
        container.list_blobs.return_value = iter([])
        adapter._container_client = container
        assert adapter.test_connection() is True


@pytest.mark.gcs
class TestGCS:
    @staticmethod
    def _service_account():
        pytest.importorskip("google.cloud.storage")
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
        ).decode()
        return {
            "type": "service_account", "project_id": "p", "private_key_id": "k", "private_key": pem,
            "client_email": "reader@p.iam.gserviceaccount.com", "client_id": "1",
            "token_uri": "https://oauth2.googleapis.com/token",
        }

    def test_registered(self):
        pytest.importorskip("google.cloud.storage")
        assert "gcs" in list_available_backends()

    def test_signed_url_offline(self):
        info = self._service_account()
        adapter = get_storage_adapter("gcs", {"bucket_name": "bkt"}, {"service_account_info": json.dumps(info)}, read_only=True)
        assert adapter.can_presign
        url = adapter.generate_presigned_url("x/y.jpg", expiration=900).url
        assert url.startswith("https://storage.googleapis.com/bkt/x/y.jpg?")
        assert "X-Goog-Expires=900" in url
        with pytest.raises(StoragePermissionError):
            adapter.delete_file("x/y.jpg")

    def test_without_key_cannot_presign(self):
        pytest.importorskip("google.cloud.storage")
        with patch("perceptra_storage.adapters.gcs.gcs.Client"):
            adapter = get_storage_adapter("gcs", {"bucket_name": "bkt"})
        assert not adapter.can_presign
        with pytest.raises(StorageOperationError):
            adapter.generate_presigned_url("a.jpg")
