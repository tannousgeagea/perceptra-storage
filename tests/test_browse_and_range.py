"""
Ranged reads (download_range) and folder-style paged listing (list_page)
across backends: local for real, S3 via moto, Azure / GCS with mocked clients.
"""
import shutil
import tempfile
from io import BytesIO
from unittest.mock import MagicMock

import pytest

from perceptra_storage import (
    LocalStorageAdapter,
    StoragePage,
    StorageNotFoundError,
    StoragePermissionError,
    get_storage_adapter,
)


@pytest.fixture
def local():
    path = tempfile.mkdtemp()
    adapter = LocalStorageAdapter({"base_path": path})
    for key in ("a/1.jpg", "a/2.jpg", "a/b/3.jpg", "top.png"):
        adapter.upload_file(BytesIO(b"0123456789"), key)
    yield adapter, path
    shutil.rmtree(path, ignore_errors=True)


class TestLocal:
    def test_download_range(self, local):
        adapter, _ = local
        assert adapter.download_range("a/1.jpg", 2, 3) == b"234"
        with pytest.raises(StorageNotFoundError):
            adapter.download_range("missing.jpg")

    def test_list_page_folders_and_paging(self, local):
        adapter, path = local
        root = adapter.list_page("")
        assert root.prefixes == ["a/"] and [o.key for o in root.objects] == ["top.png"]

        first = adapter.list_page("a/", page_size=2)
        assert [o.key for o in first.objects] == ["a/1.jpg", "a/2.jpg"] and first.prefixes == []
        assert first.next_token == "2"
        rest = adapter.list_page("a/", page_size=2, continuation_token=first.next_token)
        seen = [o.key for o in first.objects + rest.objects] + first.prefixes + rest.prefixes
        assert sorted(seen) == ["a/1.jpg", "a/2.jpg", "a/b/"] and rest.next_token is None

    def test_read_only_wrapper_delegates(self, local):
        _, path = local
        ro = get_storage_adapter("local", {"base_path": path}, read_only=True)
        assert ro.download_range("top.png", 0, 4) == b"0123"
        assert isinstance(ro.list_page("a/"), StoragePage)


@pytest.mark.s3
class TestS3:
    def test_range_and_list_page(self):
        boto3 = pytest.importorskip("boto3")
        moto = pytest.importorskip("moto")
        with moto.mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket="bkt-test")
            for key in ("imgs/1.jpg", "imgs/2.jpg", "imgs/sub/3.jpg"):
                s3.put_object(Bucket="bkt-test", Key=key, Body=b"0123456789")
            adapter = get_storage_adapter("s3", {"bucket_name": "bkt-test"},
                                          {"access_key_id": "a", "secret_access_key": "b"}, read_only=True)
            assert adapter.download_range("imgs/1.jpg", 1, 3) == b"123"
            page = adapter.list_page("imgs/", page_size=1)
            assert page.next_token is not None
            collected, token = [], None
            while True:
                page = adapter.list_page("imgs/", page_size=1, continuation_token=token)
                collected += [o.key for o in page.objects] + page.prefixes
                token = page.next_token
                if not token:
                    break
            assert sorted(collected) == ["imgs/1.jpg", "imgs/2.jpg", "imgs/sub/"]


@pytest.mark.azure
class TestAzure:
    CONFIG = {"account_name": "acct", "container_name": "c"}

    def test_range_uses_offset_and_length(self):
        pytest.importorskip("azure.storage.blob")
        adapter = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=r&sig=x"})
        container = MagicMock()
        container.get_blob_client.return_value.download_blob.return_value.readall.return_value = b"hdr"
        adapter._container_client = container
        assert adapter.download_range("k.jpg", 0, 1024) == b"hdr"
        container.get_blob_client.return_value.download_blob.assert_called_with(offset=0, length=1024)

    def test_list_page_splits_prefixes_and_objects(self):
        blob = pytest.importorskip("azure.storage.blob")
        adapter = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=rl&sig=x"})
        prefix = blob.BlobPrefix(prefix="imgs/sub/")
        prefix.name = "imgs/sub/"
        item = MagicMock(spec=["name", "size", "last_modified", "etag", "content_settings"])
        item.name, item.size, item.etag, item.content_settings = "imgs/1.jpg", 10, '"e"', None
        pages = MagicMock()
        pages.__next__ = MagicMock(return_value=iter([prefix, item]))
        pages.continuation_token = "tok-2"
        container = MagicMock()
        container.walk_blobs.return_value.by_page.return_value = pages
        adapter._container_client = container
        page = adapter.list_page("imgs/", page_size=50, continuation_token="tok-1")
        assert page.prefixes == ["imgs/sub/"] and [o.key for o in page.objects] == ["imgs/1.jpg"]
        assert page.next_token == "tok-2"
        container.walk_blobs.return_value.by_page.assert_called_with(continuation_token="tok-1")

    def test_list_without_list_permission_is_a_permission_error(self):
        pytest.importorskip("azure.storage.blob")
        from azure.core.exceptions import HttpResponseError

        adapter = get_storage_adapter("azure", self.CONFIG, {"sas_token": "sv=1&sp=r&sig=x"})
        denied = HttpResponseError(message="denied")
        denied.status_code = 403
        container = MagicMock()
        container.walk_blobs.side_effect = denied
        adapter._container_client = container
        with pytest.raises(StoragePermissionError, match="sp=rl"):
            adapter.list_page("")
