from speedict import Rdict, Options, SstFileWriter
import random

# generate some rand bytes
rand_bytes1 = [random.randbytes(200) for _ in range(100000)]
rand_bytes1.sort()
rand_bytes2 = [random.randbytes(200) for _ in range(100000)]
rand_bytes2.sort()

# write to file1.sst
writer = SstFileWriter(options=Options(raw_mode=True))
writer.open("file1.sst")
for k, v in zip(rand_bytes1, rand_bytes1):
    writer[k] = v

writer.finish()

# write to file2.sst
writer = SstFileWriter(options=Options(raw_mode=True))
writer.open("file2.sst")
for k, v in zip(rand_bytes2, rand_bytes2):
    writer[k] = v

writer.finish()

# Create a new Rdict with default options
d = Rdict("tmp", options=Options(raw_mode=True))
d.ingest_external_file(["file1.sst", "file2.sst"])
d.close()

# reopen, check if all key-values are there
d = Rdict("tmp", options=Options(raw_mode=True))
for k in rand_bytes2 + rand_bytes1:
    assert d[k] == k

d.close()

# delete tmp
Rdict.destroy("tmp")
