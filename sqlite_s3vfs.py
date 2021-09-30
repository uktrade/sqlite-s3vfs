import uuid
import boto3
import apsw


class S3VFS(apsw.VFS):        
    def __init__(self, bucket, block_size=4096):
        self.name = f's3vfs-{str(uuid.uuid4())}'
        self._bucket = bucket
        self._block_size = block_size
        super().__init__(name=self.name, base='')

    def xAccess(self, pathname, flags):
        if flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]:
            return any(self._bucket.objects.filter(Prefix=pathname))
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READWRITE"]:
            # something sometihng ACLs
            return True
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READ"]:
            # something something ACLs
            return True

    def xDelete(self, filename, syncdir):
        self._bucket.objects.filter(Prefix=filename).delete()

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self._bucket, self._block_size)


class S3VFSFile:
    def __init__(self, name, flags, bucket, block_size):
        if isinstance(name, apsw.URIFilename):
            self._key = name.filename()
        else:
            self._key = name
        self._bucket = bucket
        self._block_size = block_size

    def _blocks(self, offset, amount):
        while amount > 0:
            block = offset // self._block_size  # which block to get
            start = offset % self._block_size   # place in block to start
            consume = min(self._block_size - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def _block_object(self, block):
        return self._bucket.Object(self._key + "/" + str(block))

    def _block(self, block):
        try:
            data = self._block_object(block).get()["Body"].read()
        except self._bucket.meta.client.exceptions.NoSuchKey as e:
            data = b"".join([b"\x00"] * self._block_size)

        assert type(data) is bytes
        assert len(data) == self._block_size
        return data

    def _read(self, amount, offset):
        for block, start, consume in self._blocks(offset, amount):
            data = self._block(block)
            yield data[start:start+consume]

    def xRead(self, amount, offset):
        return b"".join(self._read(amount, offset))

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
        return sum(o.size for o in self._bucket.objects.filter(Prefix=self._key + "/"))

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        return True

    def xWrite(self, data, offset):
        for block, start, write in self._blocks(offset, len(data)):
            assert write <= len(data)

            full_data = self._block(block)
            new_data = b"".join([
                full_data[0:start],
                data,
                full_data[start+write:],
            ])
            assert len(new_data) == self._block_size

            self._block_object(block).put(
                Body=new_data,
            )
