"""
SquirrelDB Object Storage Client
S3-compatible storage operations
"""

from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, BinaryIO, Optional, Union
import aiohttp
import hashlib
import hmac
from urllib.parse import quote


@dataclass
class Bucket:
    """Storage bucket"""
    name: str
    created_at: datetime


@dataclass
class Object:
    """Storage object"""
    key: str
    size: int
    etag: str
    last_modified: datetime
    content_type: Optional[str] = None


@dataclass
class MultipartUpload:
    """Multipart upload info"""
    upload_id: str
    bucket: str
    key: str


@dataclass
class UploadPart:
    """Uploaded part info"""
    part_number: int
    etag: str


class Storage:
    """
    SquirrelDB Object Storage client.
    S3-compatible API for bucket and object operations.
    """

    def __init__(
        self,
        endpoint: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "us-east-1",
    ):
        self._endpoint = endpoint.rstrip("/")
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "Storage":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def _ensure_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()

    def _sign_request(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        payload_hash: str = "UNSIGNED-PAYLOAD",
    ) -> dict[str, str]:
        """Sign request using AWS Signature Version 4"""
        if not self._access_key or not self._secret_key:
            return headers

        now = datetime.utcnow()
        date_stamp = now.strftime("%Y%m%d")
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")

        headers["x-amz-date"] = amz_date
        headers["x-amz-content-sha256"] = payload_hash

        # Create canonical request
        canonical_uri = quote(path, safe="/")
        canonical_querystring = ""
        signed_headers = ";".join(sorted(h.lower() for h in headers.keys()))
        canonical_headers = "\n".join(
            f"{k.lower()}:{v}" for k, v in sorted(headers.items())
        ) + "\n"

        canonical_request = "\n".join([
            method,
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])

        # Create string to sign
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self._region}/s3/aws4_request"
        string_to_sign = "\n".join([
            algorithm,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ])

        # Calculate signature
        def sign(key: bytes, msg: str) -> bytes:
            return hmac.new(key, msg.encode(), hashlib.sha256).digest()

        k_date = sign(f"AWS4{self._secret_key}".encode(), date_stamp)
        k_region = sign(k_date, self._region)
        k_service = sign(k_region, "s3")
        k_signing = sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

        # Create authorization header
        auth_header = (
            f"{algorithm} "
            f"Credential={self._access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        headers["Authorization"] = auth_header

        return headers

    # =========================================================================
    # Bucket Operations
    # =========================================================================

    async def list_buckets(self) -> list[Bucket]:
        """List all buckets"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        headers = self._sign_request("GET", "/", headers)

        async with self._session.get(f"{self._endpoint}/", headers=headers) as resp:
            resp.raise_for_status()
            # Parse XML response (simplified)
            text = await resp.text()
            buckets = []
            # Simple XML parsing for bucket names
            import re
            for match in re.finditer(r"<Name>([^<]+)</Name>", text):
                name = match.group(1)
                buckets.append(Bucket(name=name, created_at=datetime.utcnow()))
            return buckets

    async def create_bucket(self, name: str) -> None:
        """Create a new bucket"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        headers = self._sign_request("PUT", f"/{name}", headers)

        async with self._session.put(f"{self._endpoint}/{name}", headers=headers) as resp:
            resp.raise_for_status()

    async def delete_bucket(self, name: str) -> None:
        """Delete a bucket (must be empty)"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        headers = self._sign_request("DELETE", f"/{name}", headers)

        async with self._session.delete(f"{self._endpoint}/{name}", headers=headers) as resp:
            resp.raise_for_status()

    async def bucket_exists(self, name: str) -> bool:
        """Check if a bucket exists"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        headers = self._sign_request("HEAD", f"/{name}", headers)

        async with self._session.head(f"{self._endpoint}/{name}", headers=headers) as resp:
            return resp.status == 200

    # =========================================================================
    # Object Operations
    # =========================================================================

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> list[Object]:
        """List objects in a bucket"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}"
        headers = self._sign_request("GET", path, headers)

        params = {"max-keys": str(max_keys)}
        if prefix:
            params["prefix"] = prefix

        async with self._session.get(
            f"{self._endpoint}/{bucket}",
            headers=headers,
            params=params,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()
            objects = []
            # Simple XML parsing
            import re
            for match in re.finditer(
                r"<Key>([^<]+)</Key>.*?<Size>(\d+)</Size>.*?<ETag>([^<]+)</ETag>",
                text,
                re.DOTALL,
            ):
                objects.append(Object(
                    key=match.group(1),
                    size=int(match.group(2)),
                    etag=match.group(3).strip('"'),
                    last_modified=datetime.utcnow(),
                ))
            return objects

    async def get_object(self, bucket: str, key: str) -> bytes:
        """Get object content"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}/{key}"
        headers = self._sign_request("GET", path, headers)

        async with self._session.get(f"{self._endpoint}{path}", headers=headers) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def get_object_stream(self, bucket: str, key: str) -> AsyncIterator[bytes]:
        """Stream object content"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}/{key}"
        headers = self._sign_request("GET", path, headers)

        async with self._session.get(f"{self._endpoint}{path}", headers=headers) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(8192):
                yield chunk

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, str, BinaryIO],
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload an object. Returns ETag."""
        await self._ensure_session()

        if isinstance(data, str):
            data = data.encode()
        elif hasattr(data, "read"):
            data = data.read()

        payload_hash = hashlib.sha256(data).hexdigest()
        headers = {
            "Host": self._endpoint.split("://")[-1],
            "Content-Type": content_type,
            "Content-Length": str(len(data)),
        }
        path = f"/{bucket}/{key}"
        headers = self._sign_request("PUT", path, headers, payload_hash)

        async with self._session.put(
            f"{self._endpoint}{path}",
            headers=headers,
            data=data,
        ) as resp:
            resp.raise_for_status()
            return resp.headers.get("ETag", "").strip('"')

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}/{key}"
        headers = self._sign_request("DELETE", path, headers)

        async with self._session.delete(f"{self._endpoint}{path}", headers=headers) as resp:
            resp.raise_for_status()

    async def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> str:
        """Copy an object. Returns new ETag."""
        await self._ensure_session()
        headers = {
            "Host": self._endpoint.split("://")[-1],
            "x-amz-copy-source": f"/{source_bucket}/{source_key}",
        }
        path = f"/{dest_bucket}/{dest_key}"
        headers = self._sign_request("PUT", path, headers)

        async with self._session.put(f"{self._endpoint}{path}", headers=headers) as resp:
            resp.raise_for_status()
            return resp.headers.get("ETag", "").strip('"')

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}/{key}"
        headers = self._sign_request("HEAD", path, headers)

        async with self._session.head(f"{self._endpoint}{path}", headers=headers) as resp:
            return resp.status == 200

    # =========================================================================
    # Multipart Upload
    # =========================================================================

    async def create_multipart_upload(
        self,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
    ) -> MultipartUpload:
        """Initiate a multipart upload"""
        await self._ensure_session()
        headers = {
            "Host": self._endpoint.split("://")[-1],
            "Content-Type": content_type,
        }
        path = f"/{bucket}/{key}?uploads"
        headers = self._sign_request("POST", path, headers)

        async with self._session.post(
            f"{self._endpoint}/{bucket}/{key}?uploads",
            headers=headers,
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()
            import re
            match = re.search(r"<UploadId>([^<]+)</UploadId>", text)
            if not match:
                raise ValueError("Failed to parse upload ID")
            return MultipartUpload(
                upload_id=match.group(1),
                bucket=bucket,
                key=key,
            )

    async def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        data: bytes,
    ) -> UploadPart:
        """Upload a part of a multipart upload"""
        await self._ensure_session()
        payload_hash = hashlib.sha256(data).hexdigest()
        headers = {
            "Host": self._endpoint.split("://")[-1],
            "Content-Length": str(len(data)),
        }
        path = f"/{bucket}/{key}"
        headers = self._sign_request("PUT", path, headers, payload_hash)

        async with self._session.put(
            f"{self._endpoint}/{bucket}/{key}?partNumber={part_number}&uploadId={upload_id}",
            headers=headers,
            data=data,
        ) as resp:
            resp.raise_for_status()
            etag = resp.headers.get("ETag", "").strip('"')
            return UploadPart(part_number=part_number, etag=etag)

    async def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[UploadPart],
    ) -> str:
        """Complete a multipart upload. Returns ETag."""
        await self._ensure_session()

        # Build completion XML
        parts_xml = "\n".join(
            f"<Part><PartNumber>{p.part_number}</PartNumber><ETag>{p.etag}</ETag></Part>"
            for p in sorted(parts, key=lambda p: p.part_number)
        )
        body = f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>"
        body_bytes = body.encode()

        payload_hash = hashlib.sha256(body_bytes).hexdigest()
        headers = {
            "Host": self._endpoint.split("://")[-1],
            "Content-Type": "application/xml",
            "Content-Length": str(len(body_bytes)),
        }
        path = f"/{bucket}/{key}"
        headers = self._sign_request("POST", path, headers, payload_hash)

        async with self._session.post(
            f"{self._endpoint}/{bucket}/{key}?uploadId={upload_id}",
            headers=headers,
            data=body_bytes,
        ) as resp:
            resp.raise_for_status()
            return resp.headers.get("ETag", "").strip('"')

    async def abort_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
    ) -> None:
        """Abort a multipart upload"""
        await self._ensure_session()
        headers = {"Host": self._endpoint.split("://")[-1]}
        path = f"/{bucket}/{key}"
        headers = self._sign_request("DELETE", path, headers)

        async with self._session.delete(
            f"{self._endpoint}/{bucket}/{key}?uploadId={upload_id}",
            headers=headers,
        ) as resp:
            resp.raise_for_status()

    async def upload_large_object(
        self,
        bucket: str,
        key: str,
        data: Union[bytes, BinaryIO],
        part_size: int = 5 * 1024 * 1024,  # 5MB default
        content_type: str = "application/octet-stream",
    ) -> str:
        """
        Upload a large object using multipart upload.
        Automatically splits data into parts. Returns ETag.
        """
        if hasattr(data, "read"):
            data = data.read()

        if len(data) <= part_size:
            return await self.put_object(bucket, key, data, content_type)

        upload = await self.create_multipart_upload(bucket, key, content_type)
        parts: list[UploadPart] = []

        try:
            part_number = 1
            offset = 0
            while offset < len(data):
                chunk = data[offset:offset + part_size]
                part = await self.upload_part(
                    bucket, key, upload.upload_id, part_number, chunk
                )
                parts.append(part)
                part_number += 1
                offset += part_size

            return await self.complete_multipart_upload(
                bucket, key, upload.upload_id, parts
            )
        except Exception:
            await self.abort_multipart_upload(bucket, key, upload.upload_id)
            raise

    async def close(self):
        """Close the storage client"""
        if self._session:
            await self._session.close()
            self._session = None
