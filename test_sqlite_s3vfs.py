import apsw
import boto3
import pytest

from sqlite_s3vfs import S3VFS

SIZES = [4096, 8192, 16384, 32768, 65536]

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


@pytest.mark.parametrize(
    'block_size', SIZES
)
def test_dummy(bucket, block_size):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)

    with apsw.Connection("/a-test/cool.db", vfs=s3vfs.name) as db:
        cursor = db.cursor()
        cursor.execute('''
            CREATE TABLE foo(x,y);
            INSERT INTO foo VALUES(1,2);
        ''')
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)]

    with apsw.Connection("/a-test/cool.db", vfs=s3vfs.name) as db:
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)]
