import os
import tempfile
import uuid
from contextlib import closing, contextmanager

import apsw
import boto3
import sqlite3
import pytest

from sqlite_s3vfs import S3VFS

PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
BLOCK_SIZES = [4095, 4096, 4097]
JOURNAL_MODES = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'OFF']


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


@contextmanager
def transaction(cursor):
    cursor.execute('BEGIN;')
    try:
        yield cursor
    except:
        cursor.execute('ROLLBACK;')
        raise
    else:
        cursor.execute('COMMIT;')


def set_pragmas(cursor, page_size, journal_mode):
    sqls = [
        f'PRAGMA page_size = {page_size};',
        f'PRAGMA journal_mode = {journal_mode};',
    ]
    for sql in sqls:
        cursor.execute(sql)


def create_db(cursor):
    sqls = [
        'CREATE TABLE foo(x,y);',
        'INSERT INTO foo VALUES ' + ','.join('(1,2)' for _ in range(0, 100)) + ';',
    ] + [
        f'CREATE TABLE foo_{i}(x,y);' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


def empty_db(cursor):
    sqls = [
        'DROP TABLE foo;'
    ] + [
        f'DROP TABLE foo_{i};' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'block_size', BLOCK_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_s3vfs(bucket, page_size, block_size, journal_mode):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)

    # Create a database and query it
    with closing(apsw.Connection("a-test/cool.db", vfs=s3vfs.name)) as db:
        set_pragmas(db.cursor(), page_size, journal_mode)

        with transaction(db.cursor()) as cursor:
            create_db(cursor)

        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Query an existing database
    with \
            closing(apsw.Connection("a-test/cool.db", vfs=s3vfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Serialize a database with serialize_fileobj, upload to S3, download it, and query it
    with \
            tempfile.NamedTemporaryFile() as fp_s3vfs:

        target_obj = bucket.Object('target/cool.sqlite')
        target_obj.upload_fileobj(s3vfs.serialize_fileobj(key_prefix='a-test/cool.db'))

        fp_s3vfs.write(target_obj.get()['Body'].read())
        fp_s3vfs.flush()

        with \
                closing(sqlite3.connect(fp_s3vfs.name)) as db, \
                transaction(db.cursor()) as cursor:

            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [(1, 2)] * 100

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

    # Serialize a database with serialize_iter and query it
    with \
            tempfile.NamedTemporaryFile() as fp_s3vfs, \
            tempfile.NamedTemporaryFile() as fp_sqlite3:

        for chunk in s3vfs.serialize_iter(key_prefix='a-test/cool.db'):
            # Empty chunks can be treated as EOF, so never output those
            assert bool(chunk)
            fp_s3vfs.write(chunk)

        fp_s3vfs.flush()

        with \
                closing(sqlite3.connect(fp_s3vfs.name)) as db, \
                transaction(db.cursor()) as cursor:

            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [(1, 2)] * 100

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        # Serialized form should be the same length as one constructed without the VFS...
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_s3vfs.name) == os.path.getsize(fp_sqlite3.name)

        # ...including after a VACUUM (which cannot be in a transaction)
        with closing(apsw.Connection("a-test/cool.db", vfs=s3vfs.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)
            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        fp_s3vfs.truncate(0)
        fp_s3vfs.seek(0)

        for chunk in s3vfs.serialize_iter(key_prefix='a-test/cool.db'):
            assert bool(chunk)
            fp_s3vfs.write(chunk)

        fp_s3vfs.flush()

        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)

            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_s3vfs.name) == os.path.getsize(fp_sqlite3.name)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'block_size', BLOCK_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_deserialize_iter(bucket, page_size, block_size, journal_mode):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)

    with tempfile.NamedTemporaryFile() as fp_sqlite3:
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)

        s3vfs.deserialize_iter(key_prefix='another-test/cool.db', bytes_iter=fp_sqlite3)

    with \
            closing(apsw.Connection('another-test/cool.db', vfs=s3vfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]


@pytest.mark.parametrize(
    'page_size', [65536]
)
@pytest.mark.parametrize(
    'block_size', [65536]
)
def test_byte_lock_page(bucket, page_size, block_size):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)
    empty = (bytes(4050),)

    with closing(apsw.Connection('another-test/cool.db', vfs=s3vfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')

        with transaction(db.cursor()) as cursor:
            cursor.execute('CREATE TABLE foo(content BLOB);')
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]

        cursor.execute('DELETE FROM foo;')
        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [] 

        with transaction(db.cursor()) as cursor:
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]


def test_set_temp_store_which_calls_xaccess(bucket):
    s3vfs = S3VFS(bucket=bucket)
    with closing(apsw.Connection('another-test/cool.db', vfs=s3vfs.name)) as db:
        db.cursor().execute("pragma temp_store_directory = 'my-temp-store'")


@pytest.mark.parametrize(
    'page_size', [4096]
)
@pytest.mark.parametrize(
    'block_size', [4095, 4096, 4097]
)
@pytest.mark.parametrize(
    'journal_mode', [journal_mode for journal_mode in JOURNAL_MODES if journal_mode != 'OFF']
)
def test_rollback(bucket, page_size, block_size, journal_mode):
    s3vfs = S3VFS(bucket=bucket, block_size=block_size)

    with closing(apsw.Connection('another-test/cool.db', vfs=s3vfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')
        db.cursor().execute(f'PRAGMA journal_mode = {journal_mode};')
        db.cursor().execute('CREATE TABLE foo(content text);')

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("INSERT INTO foo VALUES ('hello');");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('hello',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == []

        cursor.execute("INSERT INTO foo VALUES ('hello');");
        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [('hello',)]

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("UPDATE foo SET content='goodbye'");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('goodbye',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [('hello',)]
