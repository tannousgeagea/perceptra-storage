"""
Read-only wrapper for storage adapters.

External storage (a bucket or container owned by another system and only
*referenced* by Perceptra) must never be written to or deleted from, whatever
permissions its credential happens to grant. Wrapping its adapter enforces
that in one place: uploads and deletes raise StoragePermissionError, every
read is delegated unchanged.
"""
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional

from .base import BaseStorageAdapter, PresignedUrl, StorageObject, StoragePermissionError

WRITE_METHODS = ("PUT", "POST", "DELETE")


class ReadOnlyStorageAdapter(BaseStorageAdapter):
    """Delegates reads to `inner`; refuses uploads, deletes and write URLs."""

    def __init__(self, inner: BaseStorageAdapter):
        # No super().__init__: config and credentials stay on the inner adapter.
        self._inner = inner
        self.config = inner.config
        self.credentials = {}

    def _validate_config(self) -> None:  # pragma: no cover - validated by the inner adapter
        pass

    @property
    def inner(self) -> BaseStorageAdapter:
        return self._inner

    @property
    def can_presign(self) -> bool:
        return self._inner.can_presign

    @property
    def read_only(self) -> bool:
        return True

    def test_connection(self, timeout: int = 10) -> bool:
        return self._inner.test_connection(timeout=timeout)

    def upload_file(
        self,
        file_obj: BinaryIO,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        raise StoragePermissionError(f"Storage is read-only: refusing to upload '{key}'")

    def download_file(self, key: str, destination: Optional[Path] = None) -> bytes:
        return self._inner.download_file(key, destination)

    def delete_file(self, key: str) -> bool:
        raise StoragePermissionError(f"Storage is read-only: refusing to delete '{key}'")

    def file_exists(self, key: str) -> bool:
        return self._inner.file_exists(key)

    def get_file_metadata(self, key: str) -> StorageObject:
        return self._inner.get_file_metadata(key)

    def list_files(self, prefix: str = "", max_results: int = 1000) -> list[StorageObject]:
        return self._inner.list_files(prefix=prefix, max_results=max_results)

    def generate_presigned_url(self, key: str, expiration: int = 3600, method: str = "GET") -> PresignedUrl:
        if method.upper() in WRITE_METHODS:
            raise StoragePermissionError(f"Storage is read-only: refusing a {method.upper()} URL for '{key}'")
        return self._inner.generate_presigned_url(key, expiration=expiration, method=method)

    def get_public_url(self, key: str) -> Optional[str]:
        return self._inner.get_public_url(key)

    def __getattr__(self, name: str) -> Any:
        # Backend-specific helpers (e.g. stream readers) stay reachable.
        if name == "_inner":
            raise AttributeError(name)
        return getattr(self._inner, name)

    def __repr__(self) -> str:
        return f"<ReadOnly {self._inner!r}>"
