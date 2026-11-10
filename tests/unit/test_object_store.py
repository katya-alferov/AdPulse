from botocore.exceptions import ClientError

from adpulse.config import S3Settings
from adpulse.storage.object_store import ObjectStore


class FakeS3:
    def __init__(self, exists):
        self.exists = exists
        self.created = []

    def head_bucket(self, Bucket):
        if not self.exists:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    def create_bucket(self, Bucket):
        self.created.append(Bucket)


def test_creates_missing_bucket():
    fake = FakeS3(exists=False)
    ObjectStore(S3Settings(bucket="lake"), client=fake).ensure_bucket()
    assert fake.created == ["lake"]


def test_existing_bucket_is_left_alone():
    fake = FakeS3(exists=True)
    ObjectStore(S3Settings(bucket="lake"), client=fake).ensure_bucket()
    assert fake.created == []
