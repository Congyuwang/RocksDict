# RocksDict

**Key-value storage supporting any python object**

![CI](https://github.com/Congyuwang/RocksDict/actions/workflows/CI.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/dm/rocksdict)
![PyPI](https://img.shields.io/pypi/wheel/rocksdict)

## Abstract

This package enables users to store, query, and delete
a large number of key-value pairs on disk.

This is especially useful when the data cannot fit into RAM.
If you have hundreds of GBs or many TBs of key-value data to store
and query from, this is the package for you.

### Installation

This package is built for macOS (x86/arm), Windows 64/32, and Linux x86.
It can be installed from pypi with `pip install rocksdict`.

## Introduction

Below is a code example that shows how to do the following:

- Create Rdict
- Store something on disk
- Close Rdict
- Open Rdict again
- Check Rdict elements
- Iterate from Rdict
- Batch get
- Delete storage

```python
from rocksdict import Rdict
import numpy as np
import pandas as pd

path = str("./test_dict")

# create a Rdict with default options at `path`
db = Rdict(path)

db[1.0] = 1
db[1] = 1.0
db["huge integer"] = 2343546543243564534233536434567543
db["good"] = True
db["bad"] = False
db["bytes"] = b"bytes"
db["this is a list"] = [1, 2, 3]
db["store a dict"] = {0: 1}
db[b"numpy"] = np.array([1, 2, 3])
db["a table"] = pd.DataFrame({"a": [1, 2], "b": [2, 1]})

# close Rdict
db.close()

# reopen Rdict from disk
db = Rdict(path)
assert db[1.0] == 1
assert db[1] == 1.0
assert db["huge integer"] == 2343546543243564534233536434567543
assert db["good"] == True
assert db["bad"] == False
assert db["bytes"] == b"bytes"
assert db["this is a list"] == [1, 2, 3]
assert db["store a dict"] == {0: 1}
assert np.all(db[b"numpy"] == np.array([1, 2, 3]))
assert np.all(db["a table"] == pd.DataFrame({"a": [1, 2], "b": [2, 1]}))

# iterate through all elements
for k, v in db.items():
    print(f"{k} -> {v}")

# batch get:
print(db[["good", "bad", 1.0]])
# [True, False, 1]
 
# delete Rdict from dict
db.close()
Rdict.destroy(path)
```

Supported types:

- key: `int, float, bool, str, bytes`
- value: `int, float, bool, str, bytes` and anything that
    supports `pickle`.

## Rocksdb Options

Since the backend is implemented using rocksdb,
most of rocksdb options are supported:

### Example of tuning

```python
from rocksdict import Rdict, Options, SliceTransform, PlainTableFactoryOptions
import os

def db_options():
    opt = Options()
    # create table
    opt.create_if_missing(True)
    # config to more jobs
    opt.set_max_background_jobs(os.cpu_count())
    # configure mem-table to a large value (256 MB)
    opt.set_write_buffer_size(0x10000000)
    opt.set_level_zero_file_num_compaction_trigger(4)
    # configure l0 and l1 size, let them have the same size (1 GB)
    opt.set_max_bytes_for_level_base(0x40000000)
    # 256 MB file size
    opt.set_target_file_size_base(0x10000000)
    # use a smaller compaction multiplier
    opt.set_max_bytes_for_level_multiplier(4.0)
    # use 8-byte prefix (2 ^ 64 is far enough for transaction counts)
    opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
    # set to plain-table for better performance
    opt.set_plain_table_factory(PlainTableFactoryOptions())
    return opt

db = Rdict(str("./some_path"), db_options())
```

### Example of Bulk Writing By SstFileWriter

```python
from rocksdict import Rdict, Options, SstFileWriter
import random

# generate some rand bytes
rand_bytes1 = [random.randbytes(200) for _ in range(100000)]
rand_bytes1.sort()
rand_bytes2 = [random.randbytes(200) for _ in range(100000)]
rand_bytes2.sort()

# write to file1.sst
writer = SstFileWriter()
writer.open("file1.sst")
for k, v in zip(rand_bytes1, rand_bytes1):
    writer[k] = v

writer.finish()

# write to file2.sst
writer = SstFileWriter(Options())
writer.open("file2.sst")
for k, v in zip(rand_bytes2, rand_bytes2):
    writer[k] = v

writer.finish()

# Create a new Rdict with default options
d = Rdict("tmp")
d.ingest_external_file(["file1.sst", "file2.sst"])
d.close()

# reopen, check if all key-values are there
d = Rdict("tmp")
for k in rand_bytes2 + rand_bytes1:
    assert d[k] == k

d.close()

# delete tmp
Rdict.destroy("tmp")
```
