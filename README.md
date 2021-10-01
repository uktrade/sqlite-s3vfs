# sqlite-s3vfs [![CircleCI](https://circleci.com/gh/uktrade/sqlite-s3vfs.svg?style=shield)](https://circleci.com/gh/uktrade/sqlite-s3vfs) [![Test Coverage](https://api.codeclimate.com/v1/badges/6df8a84b0ff21d7ecf22/test_coverage)](https://codeclimate.com/github/uktrade/sqlite-s3vfs/test_coverage)

Virtual filesystem for SQLite to read from and write to S3


## Installation

sqlite-s3vfs depends on [APSW](https://github.com/rogerbinns/apsw), which is not officially available on PyPI, but can be installed directly from GitHub.

```bash
pip install sqlite-s3vfs
pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all --global-option=build --global-option=--enable-all-extensions
```


## Usage

sqlite-s3vfs is an [APSW](https://rogerbinns.github.io/apsw/) virtual filesystem that requires [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for its communication with S3.

```python
import apsw
import boto3
import sqlite_s3vfs

# A boto3 bucket resource
bucket = boto3.Session().resource('s3').Bucket('my-bucket')

# An S3VFS for that bucket
s3vfs =  sqlite_s3vfs.S3VFS(bucket=bucket)

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

# Get the serialized form of the sqlite file, say to upload to S3 as a single object
for chunk in s3vfs.serialize(key_prefix=key_prefix):
    print(chunk)
```

See the [APSW documentation](https://rogerbinns.github.io/apsw/) for more examples.


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
```
./stop-minio.sh
```
