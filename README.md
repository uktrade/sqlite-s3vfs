# sqlite-s3vfs [![CircleCI](https://circleci.com/gh/uktrade/sqlite-s3vfs.svg?style=shield)](https://circleci.com/gh/uktrade/sqlite-s3vfs) [![Test Coverage](https://api.codeclimate.com/v1/badges/6df8a84b0ff21d7ecf22/test_coverage)](https://codeclimate.com/github/uktrade/sqlite-s3vfs/test_coverage)

Virtual filesystem for SQLite to read and write to S3


# Installation

sqlite-s3vfs depends on [APSW](https://github.com/rogerbinns/apsw), which is not officially available on PyPI, but can be installed directly from GitHub.

```bash
pip install sqlite-s3vfs
pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all --global-option=build --global-option=--enable-all-extensions
```
