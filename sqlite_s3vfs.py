import uuid
import apsw


class S3VFS(apsw.VFS):        
    def __init__(self, bucket, block_size=4096):
        self.name = f's3vfs-{str(uuid.uuid4())}'
        self._bucket = bucket
        self._block_size = block_size
        super().__init__(name=self.name, base='')

    def xAccess(self, pathname, flags):
        return (
            flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
            and any(self._bucket.objects.filter(Prefix=pathname + '/'))
        ) or (
            flags != apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
        )

    def xFullPathname(self, filename):
        return filename

    def xDelete(self, filename, syncdir):
        self._bucket.objects.filter(Prefix=filename + '/').delete()

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self._bucket, self._block_size)

    def serialize_iter(self, key_prefix):
        for obj in self._bucket.objects.filter(Prefix=key_prefix + '/'):
            yield from obj.get()['Body'].iter_chunks()

    def serialize_fileobj(self, key_prefix):
        chunk = b''
        offset = 0
        it = iter(self.serialize_iter(key_prefix))

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, n=-1):
                n = \
                    n if n != -1 else \
                    4294967294 * 65536  # max size of SQLite file
                return b''.join(up_to_iter(n))

        return FileLikeObj()

    def deserialize_iter(self, key_prefix, bytes_iter):
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        def block_bytes_iter():
            while True:
                block = b''.join(up_to_iter(self._block_size))
                if not block:
                    break
                yield block

        for block, block_bytes in enumerate(block_bytes_iter()):
            self._bucket.Object(f'{key_prefix}/{block:010d}').put(Body=block_bytes)


class S3VFSFile:
    def __init__(self, name, flags, bucket, block_size):
        self._key_prefix = \
            self._key_prefix = name.filename() if isinstance(name, apsw.URIFilename) else \
            name
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
        return self._bucket.Object(f'{self._key_prefix}/{block:010d}')

    def _block_bytes(self, block):
        try:
            block_bytes = self._block_object(block).get()["Body"].read()
        except self._bucket.meta.client.exceptions.NoSuchKey as e:
            block_bytes = b''

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
        total = 0

        for obj in self._bucket.objects.filter(Prefix=self._key_prefix + "/"):
            total += obj.size
            to_keep = max(obj.size - total + newsize, 0)

            if to_keep == 0:
                obj.delete()
            elif to_keep < obj.size:
                obj.put(Body=obj.get()['Body'].read()[:to_keep])

        return True

    def xWrite(self, data, offset):
        lock_page_offset = 1073741824
        page_size = len(data)

        if offset == lock_page_offset + page_size:
            # Ensure the previous blocks have enough bytes for size calculations and serialization.
            # SQLite seems to always write pages sequentially, except that it skips the byte-lock
            # page, so we only check previous blocks if we know we're just after the byte-lock
            # page.

            data_first_block = offset // self._block_size
            lock_page_block = lock_page_offset // self._block_size
            for block in range(data_first_block - 1, lock_page_block - 1, -1):
                original_block_bytes = self._block_bytes(block)
                if len(original_block_bytes) == self._block_size:
                    break
                self._block_object(block).put(Body=original_block_bytes + bytes(
                    self._block_size - len(original_block_bytes)
                ))

        data_offset = 0
        for block, start, write in self._blocks(offset, len(data)):

            data_to_write = data[data_offset:data_offset+write]

            if start != 0 or len(data_to_write) != self._block_size:
                original_block_bytes = self._block_bytes(block)
                original_block_bytes = original_block_bytes + bytes(max(start - len(original_block_bytes), 0))

                data_to_write = \
                    original_block_bytes[0:start] + \
                    data_to_write + \
                    original_block_bytes[start+write:]

            data_offset += write
            self._block_object(block).put(Body=data_to_write)
