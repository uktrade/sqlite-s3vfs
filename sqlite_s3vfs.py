import uuid
import struct
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
            return any(self._bucket.objects.filter(Prefix=pathname + '/'))
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READWRITE"]:
            # something sometihng ACLs
            return True
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READ"]:
            # something something ACLs
            return True

    def xFullPathname(self, filename):
        return filename

    def xDelete(self, filename, syncdir):
        self._bucket.objects.filter(Prefix=filename).delete()

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self._bucket, self._block_size)

    def serialize(self, key_prefix):
        bytes_so_far = 0

        for i, obj in enumerate(self._bucket.objects.filter(Prefix=key_prefix + '/')):
            block_bytes = obj.get()['Body'].read()

            if i == 0:
                page_size, = struct.Struct('>H').unpack(block_bytes[16:18])
                page_size = 65536 if page_size == 1 else page_size
                num_pages, = struct.Struct('>L').unpack(block_bytes[28:32])
                bytes_expected = page_size * num_pages

            to_yield = min(len(block_bytes), bytes_expected - bytes_so_far)
            yield block_bytes[:to_yield]
            bytes_so_far += to_yield

class S3VFSFile:
    def __init__(self, name, flags, bucket, block_size):
        if isinstance(name, apsw.URIFilename):
            self._key_prefix = name.filename()
        else:
            self._key_prefix = name
        self._bucket = bucket
        self._block_size = block_size
        self._empty_block_bytes = bytes(self._block_size)

    def _blocks(self, offset, amount):
        while amount > 0:
            block = offset // self._block_size  # which block to get
            start = offset % self._block_size   # place in block to start
            consume = min(self._block_size - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def _block_object(self, block):
        return self._bucket.Object(f'{self._key_prefix}/{block:010d}')

    def _block_bytes(self, block):
        try:
            block_bytes = self._block_object(block).get()["Body"].read()
        except self._bucket.meta.client.exceptions.NoSuchKey as e:
            block_bytes = self._empty_block_bytes

        assert type(block_bytes) is bytes
        assert len(block_bytes) == self._block_size
        return block_bytes

    def xRead(self, amount, offset):
        def _read():
            for block, start, consume in self._blocks(offset, amount):
                block_bytes = self._block_bytes(block)
                yield block_bytes[start:start+consume]

        return b"".join(_read())

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
        return sum(o.size for o in self._bucket.objects.filter(Prefix=self._key_prefix + "/"))

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        return True

    def xWrite(self, data, offset):
        data_offset = 0
        for block, start, write in self._blocks(offset, len(data)):

            assert write <= len(data)

            if write == len(data) == self._block_size:
                # No need to fetch the original bytes, since we completely replace them
                new_block_bytes = data
            else:
                original_block_bytes = self._block_bytes(block)
                new_block_bytes = b"".join([
                    original_block_bytes[0:start],
                    data[data_offset:data_offset+write],
                    original_block_bytes[start+write:],
                ])
            data_offset += write

            assert len(new_block_bytes) == self._block_size

            self._block_object(block).put(
                Body=new_block_bytes,
            )
