import apsw
import boto3
import pytest

from sqlite_s3vfs import S3VFS


@pytest.fixture
def bucket():
    session = boto3.Session(
        aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
        aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        region_name='us-east-1',
    )
    s3 = session.resource('s3',
        endpoint_url='http://localhost:9000/'
    )
    bucket = s3.create_bucket(Bucket='my-bucket')
    yield bucket
    bucket.objects.all().delete()
    bucket.delete()


def test_dummy(bucket):
    s3vfs = S3VFS(bucket=bucket)

    db = apsw.Connection("/a-test/cool.db", vfs=s3vfs.vfsname)
    db.cursor().execute("create table foo(x,y); insert into foo values(1,2)")
