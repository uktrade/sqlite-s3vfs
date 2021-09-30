import unittest

import apsw
import boto3

from sqlite_s3vfs import S3VFS


class TestSqliteS3VFS(unittest.TestCase):

    def test_dummy(self):
        session = boto3.Session(
            aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
            aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            region_name='us-east-1',
        )
        s3 = session.resource('s3',
            endpoint_url='http://localhost:9000/'
        )
        bucket = s3.create_bucket(Bucket='my-bucket')
        s3vfs = S3VFS(
            s3=s3,
            bucket=bucket,
        )

        db=apsw.Connection("/a-test/cool.db", vfs=s3vfs.vfsname)
        db.cursor().execute("create table foo(x,y); insert into foo values(1,2)")

        bucket.objects.all().delete()
        bucket.delete()
