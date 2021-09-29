import boto3
import apsw

BLOCK_SIZE = 64 * 1024
EMPTY_BLOCK = b"".join([b"\x00"] * BLOCK_SIZE)

# Inheriting from a base of "" means the default vfs
class S3VFS(apsw.VFS):        
    def __init__(self, s3, bucket, vfsname=f"s3vfs", basevfs=""):
        self.vfsname = vfsname
        self.basevfs = basevfs
        self.s3 = s3
        self.bucket = bucket
        apsw.VFS.__init__(self, self.vfsname, self.basevfs)
        
    def xAccess(self, pathname, flags):
        if flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]:
            return any(self.bucket.objects.filter(Prefix=pathname))
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READWRITE"]:
            # something sometihng ACLs
            return True
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READ"]:
            # something something ACLs
            return True
        
    def xDelete(self, filename, syncdir):
        self.bucket.objects.filter(Prefix=filename).delete()

    def xOpen(self, name, flags):
        return S3VFSFile(self.basevfs, name, flags, self.s3, self.bucket)
    

class S3VFSFile:
    def __init__(self, inheritfromvfsname, name, flags, s3, bucket):
        if isinstance(name, apsw.URIFilename):
            self.key = name.filename()
        else:
            self.key = name
        self.s3 = s3
        self.bucket = bucket
        
    def blocks(self, offset, amount):
        while amount > 0:
            block = offset // BLOCK_SIZE  # which block to get
            start = offset % BLOCK_SIZE   # place in block to start
            consume = min(BLOCK_SIZE - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume
            
    def block_object(self, block):
        return self.s3.Object(self.bucket.name, self.key + "/" + str(block))
            
    def block(self, block):
        try:
            data = self.block_object(block).get()["Body"].read()
        except self.s3.meta.client.exceptions.NoSuchKey as e:
            data = EMPTY_BLOCK

        assert type(data) is bytes
        assert len(data) == BLOCK_SIZE
        return data
    
    def read(self, amount, offset):
        for block, start, consume in self.blocks(offset, amount):
            data = self.block(block)
            yield data[start:start+consume]
            
    def xRead(self, amount, offset):
        return b"".join(self.read(amount, offset))

    def xFileControl(self, *args):
        return False
    
    def xCheckReservedLock(self):
        return False
    
    def xLock(self, level):
        pass
        
    def xUnlock(self, level):
        pass
    
    def xClose(self):
        pass
    
    def xFileSize(self):
        return sum(o.size for o in self.bucket.objects.filter(Prefix=self.key + "/"))
    
    def xSync(self, flags):
        return True
    
    def xTruncate(self, newsize):
        return True
    
    def xWrite(self, data, offset):
        for block, start, write in self.blocks(offset, len(data)):
            assert write <= len(data)
            
            full_data = self.block(block)
            new_data = b"".join([
                full_data[0:start],
                data,
                full_data[start+write:],
            ])
            assert len(new_data) == BLOCK_SIZE

            self.block_object(block).put(
                Body=new_data,
            )
            

session = boto3.Session(profile_name='data_flow_bucket')

s3vfs = S3VFS(
    s3=session.resource('s3'),
    bucket=s3.Bucket('MY-BUCKET-NAME'),
)

db=apsw.Connection("/a-test/cool.db", vfs=s3vfs.vfsname)
db.cursor().execute("create table foo(x,y); insert into foo values(1,2)")