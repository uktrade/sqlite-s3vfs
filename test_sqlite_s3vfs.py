import tempfile
import uuid

import apsw
import boto3
import sqlite3
import pytest

from sqlite_s3vfs import S3VFS

SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
JOURNAL_MODES = ['DELETE' ,'TRUNCATE', 'PERSIST', 'MEMORY' ,'WAL', 'OFF']

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

    bucket = s3.create_bucket(Bucket=f's3vfs-{str(uuid.uuid4())}')
    yield bucket
    bucket.objects.all().delete()
    bucket.delete()


@pytest.mark.parametrize(
    'page_size', SIZES
)
@pytest.mark.parametrize(
    'block_size', SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_s3vfs(bucket, page_size, block_size, journal_mode):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)

    # Create a database and query it
    with apsw.Connection("a-test/cool.db", vfs=s3vfs.name) as db:
        cursor = db.cursor()
        cursor.execute(f'''
            PRAGMA journal_mode = {journal_mode};
        ''')
        cursor.execute(f'''
            PRAGMA page_size = {page_size};
        ''');
        cursor.execute(f'''
            CREATE TABLE foo(x,y);
            INSERT INTO foo VALUES(1,2);
        ''')
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)]

    # Query an existing database
    with apsw.Connection("a-test/cool.db", vfs=s3vfs.name) as db:
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)]

    # Serialize a database and query it
    with tempfile.NamedTemporaryFile() as fp:
        for chunk in s3vfs.serialize(key_prefix='a-test/cool.db'):
            fp.write(chunk)

        with sqlite3.connect(fp.name) as con:
            cur = con.cursor()
            cur.execute('PRAGMA page_size = {};'.format(page_size))
            cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)]
