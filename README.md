# sqlite-s3vfs [![CircleCI](https://circleci.com/gh/uktrade/sqlite-s3vfs.svg?style=shield)](https://circleci.com/gh/uktrade/sqlite-s3vfs) [![Test Coverage](https://api.codeclimate.com/v1/badges/6df8a84b0ff21d7ecf22/test_coverage)](https://codeclimate.com/github/uktrade/sqlite-s3vfs/test_coverage)

Python virtual filesystem for SQLite to read from and write to S3.

No locking is performed, so client code _must_ ensure that writes do not overlap with other writes or reads. If multiple writes happen at the same time, the database will probably become corrupt and data be lost.

Based on [simonwo's gist](https://gist.github.com/simonwo/b98dc75feb4b53ada46f224a3b26274c), and inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998), and [michalc's sqlite-s3-query](https://github.com/michalc/sqlite-s3-query).


## How does it work?

sqlite-s3vfs stores the SQLite database in fixed-sized _blocks_, and each is stored as a separate object in S3. SQLite stores its data in fixed-size _pages_, and always writes exactly a page at a time. This virtual filesystem translates  page reads and writes to block reads and writes. In the case of SQLite pages being the same size as blocks, which is the case by default, each page write results in exactly one block write.

Separate objects are required since S3 does not support the partial replace of an object; to change even 1 byte, it must be re-uploaded in full.


## Installation

sqlite-s3vfs depends on [APSW](https://github.com/rogerbinns/apsw), which is not officially available on PyPI, but can be installed directly from GitHub.

```bash
pip install sqlite-s3vfs
pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--sqlite --global-option=build --global-option=--enable-all-extensions
```

Installing APSW from GitHub can be difficult on some platforms since it involves compiling a Python extension and (depending on options chosen) SQLite itself. As an alternative, sqlite-s3vfs should work with [apsw-wheels](https://pypi.org/project/apsw-wheels/), which includes binaries for various platforms.

```bash
pip install apsw-wheels
```

However, at the time of writing aspw-wheels has no official relationship with APSW.


## Usage

sqlite-s3vfs is an [APSW](https://rogerbinns.github.io/apsw/) virtual filesystem that requires [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for its communication with S3.

```python
import apsw
import boto3
import sqlite_s3vfs

# A boto3 bucket resource
bucket = boto3.Session().resource('s3').Bucket('my-bucket')

# An S3VFS for that bucket
s3vfs = sqlite_s3vfs.S3VFS(bucket=bucket)

# sqlite-s3vfs stores many objects under this prefix
# Note that it's not typical to start a key prefix with '/'
key_prefix = 'my/path/cool.sqlite'

# Connect, insert data, and query
with apsw.Connection(key_prefix, vfs=s3vfs.name) as db:
    cursor = db.cursor()
    cursor.execute(f'''
        CREATE TABLE foo(x,y);
        INSERT INTO foo VALUES(1,2);
    ''')
    cursor.execute('SELECT * FROM foo;')
    print(cursor.fetchall())
```

See the [APSW documentation](https://rogerbinns.github.io/apsw/) for more examples.


### Serializing (getting a regular SQLite file out of the VFS)

The bytes corresponding to a regular SQLite file can be extracted with the `serialize_iter` function, which returns an iterable,

```python
for chunk in s3vfs.serialize_iter(key_prefix=key_prefix):
    print(chunk)
```

or with `serialize_fileobj`, which returns a non-seekable file-like object. This can be passed to Boto3's `upload_fileobj` method to upload a regular SQLite file to S3.

```python
target_obj = boto3.Session().resource('s3').Bucket('my-target-bucket').Object('target/cool.sqlite')
target_obj.upload_fileobj(s3vfs.serialize_fileobj(key_prefix=key_prefix))
```


### Deserializing (getting a regular SQLite file into the VFS)

```python
# Any iterable that yields bytes can be used. In this example, bytes come from
# a regular SQLite file already in S3
source_obj = boto3.Session().resource('s3').Bucket('my-source-bucket').Object('source/cool.sqlite')
bytes_iter = source_obj.get()['Body'].iter_chunks()

s3vfs.deserialize_iter(key_prefix='my/path/cool.sqlite', bytes_iter=bytes_iter)
```


## Tests

The tests require the dev dependencies and APSW to installed, and MinIO started

```bash
pip install -r requirements-dev.txt
pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all --global-option=build --global-option=--enable-all-extensions
./start-minio.sh
```

can be run with pytest

```bash
pytest
```

and finally Minio stopped

```bash
./stop-minio.sh
```
