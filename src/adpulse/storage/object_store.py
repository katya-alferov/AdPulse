"""S3-compatible object storage helper (works with SeaweedFS, MinIO or AWS S3).

Only the endpoint URL and the credentials differ between local and AWS:
  local : S3_ENDPOINT_URL=http://seaweedfs:8333
  AWS   : leave S3_ENDPOINT_URL empty and use an IAM role (no static keys)

Run:  python -m adpulse.storage.object_store          (creates the bucket)
      python -m adpulse.storage.object_store --list   (lists objects)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any, Iterator, Optional

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from adpulse.config import S3Settings, Settings
from adpulse.logging_setup import setup_logging

log = logging.getLogger("adpulse.storage.s3")


class ObjectStore:
    def __init__(self, settings: S3Settings, client: Any = None) -> None:
        self.settings = settings
        self.client = client or boto3.client(
            "s3",
            endpoint_url=settings.endpoint_url or None,  # None -> real AWS
            aws_access_key_id=settings.access_key or None,
            aws_secret_access_key=settings.secret_key or None,
            region_name=settings.region,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def ensure_bucket(self, retries: int = 30, delay_s: float = 2.0) -> None:
        """Create the bucket if it does not exist (retry while storage starts)."""
        bucket = self.settings.bucket
        for attempt in range(1, retries + 1):
            try:
                self.client.head_bucket(Bucket=bucket)
                log.info("Bucket %s already exists", bucket)
                return
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code in ("404", "NoSuchBucket", "NotFound"):
                    self.client.create_bucket(Bucket=bucket)
                    log.info("Created bucket %s", bucket)
                    return
                log.warning("S3 error (attempt %d/%d): %s", attempt, retries, exc)
            except BotoCoreError as exc:
                log.warning("S3 not reachable (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay_s)
        raise RuntimeError(f"Could not create bucket {bucket}")

    def iter_objects(self, prefix: str = "") -> Iterator[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.settings.bucket, Prefix=prefix):
            yield from page.get("Contents", [])

    def summary(self, prefix: str = "") -> dict[str, int]:
        count = size = 0
        for obj in self.iter_objects(prefix):
            if obj["Key"].endswith(".parquet"):
                count += 1
                size += obj["Size"]
        return {"parquet_files": count, "bytes": size}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="AdPulse object storage helper")
    parser.add_argument("--list", action="store_true", help="list objects instead of creating the bucket")
    parser.add_argument("--prefix", default="")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    store = ObjectStore(settings.s3)
    if args.list:
        for obj in store.iter_objects(args.prefix):
            print(f"{obj['Size']:>10}  {obj['Key']}")
        print(store.summary(args.prefix))
    else:
        store.ensure_bucket()
    return 0


if __name__ == "__main__":
    sys.exit(main())
