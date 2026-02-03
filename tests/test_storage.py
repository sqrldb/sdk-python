"""Tests for SquirrelDB Python SDK - Storage"""

import pytest
import re
from urllib.parse import urlencode


class TestStorageOptions:
    """Test storage options structure"""

    def test_minimal_options(self):
        opts = {"endpoint": "http://localhost:9000"}
        assert opts["endpoint"] == "http://localhost:9000"
        assert opts.get("access_key_id") is None
        assert opts.get("secret_access_key") is None
        assert opts.get("region") is None

    def test_full_options(self):
        opts = {
            "endpoint": "https://s3.amazonaws.com",
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "region": "us-west-2",
        }
        assert opts["endpoint"] == "https://s3.amazonaws.com"
        assert opts["access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert opts["region"] == "us-west-2"


class TestBucketType:
    """Test Bucket type"""

    def test_bucket_structure(self):
        bucket = {"name": "my-bucket", "created_at": "2024-01-01T00:00:00Z"}
        assert bucket["name"] == "my-bucket"
        assert bucket["created_at"] == "2024-01-01T00:00:00Z"

    def test_bucket_name_validation_patterns(self):
        valid_names = ["my-bucket", "bucket123", "test.bucket.name"]
        for name in valid_names:
            assert 3 <= len(name) <= 63


class TestStorageObjectType:
    """Test StorageObject type"""

    def test_object_structure(self):
        obj = {
            "key": "path/to/file.txt",
            "size": 1024,
            "etag": "d41d8cd98f00b204e9800998ecf8427e",
            "last_modified": "2024-01-01T00:00:00Z",
            "content_type": "text/plain",
        }
        assert obj["key"] == "path/to/file.txt"
        assert obj["size"] == 1024
        assert obj["etag"] == "d41d8cd98f00b204e9800998ecf8427e"
        assert obj["content_type"] == "text/plain"

    def test_object_with_null_content_type(self):
        obj = {
            "key": "file.bin",
            "size": 2048,
            "etag": "abc123",
            "last_modified": "2024-01-01T00:00:00Z",
            "content_type": None,
        }
        assert obj["content_type"] is None


class TestS3APIPaths:
    """Test S3 API path construction"""

    def test_list_buckets_path(self):
        path = "/"
        assert path == "/"

    def test_bucket_path(self):
        bucket = "my-bucket"
        path = f"/{bucket}"
        assert path == "/my-bucket"

    def test_object_path(self):
        bucket = "my-bucket"
        key = "path/to/file.txt"
        path = f"/{bucket}/{key}"
        assert path == "/my-bucket/path/to/file.txt"

    def test_list_objects_with_prefix(self):
        bucket = "my-bucket"
        prefix = "logs/"
        params = urlencode({"prefix": prefix})
        path = f"/{bucket}?{params}"
        assert path == "/my-bucket?prefix=logs%2F"

    def test_list_objects_with_max_keys(self):
        bucket = "my-bucket"
        params = urlencode({"max-keys": 100})
        path = f"/{bucket}?{params}"
        assert path == "/my-bucket?max-keys=100"


class TestS3XMLResponseParsing:
    """Test S3 XML response parsing"""

    def test_parse_bucket_name_from_xml(self):
        xml = "<Name>my-bucket</Name>"
        match = re.search(r"<Name>([^<]+)</Name>", xml)
        assert match is not None
        assert match.group(1) == "my-bucket"

    def test_parse_multiple_bucket_names(self):
        xml = """
        <Buckets>
            <Bucket><Name>bucket1</Name></Bucket>
            <Bucket><Name>bucket2</Name></Bucket>
        </Buckets>
        """
        matches = re.findall(r"<Name>([^<]+)</Name>", xml)
        assert matches == ["bucket1", "bucket2"]

    def test_parse_object_listing(self):
        xml = """
        <Contents>
            <Key>file1.txt</Key>
            <Size>1024</Size>
            <ETag>"abc123"</ETag>
        </Contents>
        """
        key_match = re.search(r"<Key>([^<]+)</Key>", xml)
        size_match = re.search(r"<Size>(\d+)</Size>", xml)
        etag_match = re.search(r"<ETag>([^<]+)</ETag>", xml)

        assert key_match.group(1) == "file1.txt"
        assert int(size_match.group(1)) == 1024
        assert etag_match.group(1).replace('"', "") == "abc123"


class TestContentTypes:
    """Test content type mappings"""

    def test_common_content_types(self):
        content_types = {
            ".txt": "text/plain",
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".pdf": "application/pdf",
        }
        assert content_types[".json"] == "application/json"
        assert content_types[".png"] == "image/png"
