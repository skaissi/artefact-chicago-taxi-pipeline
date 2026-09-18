"""S3 / MinIO adapter for immutable CSV pages and the manifest."""
import json
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


def s3_client(settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(client, bucket, attempts=30):
    """The Compose dependency starts MinIO, but does not imply readiness."""
    for attempt in range(attempts):
        try:
            client.head_bucket(Bucket=bucket)
            return
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                client.create_bucket(Bucket=bucket)
                return
            if status in (403, 400):
                raise
        except (EndpointConnectionError, ConnectionError):
            pass
        if attempt < attempts - 1:
            time.sleep(2)
    raise RuntimeError("MinIO not ready after 30 attempts (60 seconds).")


def save_manifest(client, bucket, key, manifest):
    client.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def load_manifest(client, bucket, key):
    return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
