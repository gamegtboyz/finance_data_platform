import os
import json
import tempfile
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch

import src.storage.s3_client as s3_mod
from src.storage.s3_client import s3_upload, s3_list_objects, s3_download

BUCKET = "test-finance-bucket"
REGION = "us-east-1"

@pytest.fixture
def aws_credentials(monkeypatch):
    """Mocked AWS credentials for moto library."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_REGION", REGION)

@pytest.fixture
def s3(aws_credentials):
    """Start moto mock, create bucket, patch the module-level client."""
    with mock_aws():
        real_client = boto3.client("s3", region_name=REGION)
        real_client.create_bucket(Bucket=BUCKET)

        # patch the module-level global so our wrapper functions use the moto-backed client instead of the one created at import time.
        with patch.object(s3_mod, "s3_client", real_client):
            yield real_client

class TestS3Upload:
    def test_upload_file(self, s3, tmp_path):
        """ A local file should appear in the bucket after upload. """
        local_file = tmp_path / "AAPL_2026-03-09.json"
        local_file.write_text(json.dumps({"symbol": "AAPL"}))

        s3_upload(str(local_file), BUCKET, "raw/AAPL/AAPL_2026-03-09.json")

        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert "raw/AAPL/AAPL_2026-03-09.json" in keys

    def test_upload_uses_basename_when_no_object_name(self, s3, tmp_path):
        """ When object_name is omitted, basename of local file is used as the key."""
        local_file = tmp_path / "MSFT_2026-03-09.json"
        local_file.write_text("{}")

        s3_upload(str(local_file), BUCKET)

        response = s3.list_objects_v2(Bucket=BUCKET)
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert "MSFT_2026-03-09.json" in keys

    def test_upload_raises_on_bad_bucket(self, s3, tmp_path):
        """ Uploading to a non-existent bucket must raise RuntimeError."""
        local_file = tmp_path / "test.json"
        local_file.write_text("{}")

        with pytest.raises(RuntimeError, match="Failed to upload"):
            s3_upload(str(local_file), 'bucket-does-not-exist')

class TestS3ListObjects:
    def test_returns_empty_for_missing_prefix(self, s3):
        """ List a prefix with no objects returns aan empty list, not an error. """
        result = s3_list_objects(BUCKET, prefix="nonexistent/")
        assert result == []

    def test_returns_uploaded_objects(self, s3, tmp_path):
        """ Objects uploaded under a prefix are returned by list. """
        local_file = tmp_path / "GOOGL_2026-03-09.json"
        local_file.write_text("{}")
        s3.upload_file(str(local_file), BUCKET, "raw/GOOGL/GOOGL_2026-03-09.json")

        objects = s3_list_objects(BUCKET, prefix="raw/GOOGL/")
        keys = [obj["Key"] for obj in objects]
        assert "raw/GOOGL/GOOGL_2026-03-09.json" in keys

class TestS3Download:
    def test_download_file(self, s3, tmp_path):
        """ A file in S3 should be downloadable to a local path."""
        content = json.dumps({"test": "data"})
        s3.put_object(Bucket=BUCKET, Key="raw/NVDA/test.json", Body=content)

        dest = tmp_path / "downloaded.json"
        s3_download(BUCKET, "raw/NVDA/test.json", str(dest))
        assert dest.read_text() == content

    def test_download_raises_on_missing_key(self, s3, tmp_path):
        """ Downloading a key that does not exist must raise RuntimeError. """
        dest = tmp_path / "output.json"

        with pytest.raises(RuntimeError, match="Failed to download"):
            s3_download(BUCKET, "does/not/exist.json", str(dest))
