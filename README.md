# RocksDict

**Key-value storage supporting any python object**

![CI](https://github.com/Congyuwang/RocksDict/actions/workflows/CI.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/dm/speedict)
![PyPI](https://img.shields.io/pypi/wheel/speedict)
[![Support python versions](https://img.shields.io/pypi/pyversions/speedict.svg)](https://pypi.org/project/speedict/)

## Introduction

This library has two purposes.

1. As an on-disk key-value storage solution for Python.
2. As a RocksDB interface.

These two purposes operate in different modes:

- **Default mode**, which allows storing `int`, `float`, 
`bool`, `str`, `bytes`, and other python objects (with `Pickle`).

- **Raw mode** (`options=Options(raw_mode=True)`),
which allows storing only `bytes`.

## Installation

Wheels available, just `pip install speedict`.

## Examples

### A minimal example

```python
from speedict import Rdict
import numpy as np
import pandas as pd

path = str("./test_dict")

# create a Rdict with default options at `path`
db = Rdict(path)
db[1.0] = 1
db["huge integer"] = 2343546543243564534233536434567543
db["good"] = True
db["bytes"] = b"bytes"
db["this is a list"] = [1, 2, 3]
db["store a dict"] = {0: 1}
db[b"numpy"] = np.array([1, 2, 3])
db["a table"] = pd.DataFrame({"a": [1, 2], "b": [2, 1]})

# reopen Rdict from disk
db.close()
db = Rdict(path)
assert db[1.0] == 1
assert db["huge integer"] == 2343546543243564534233536434567543
assert db["good"] == True
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

### An Example of Raw Mode

This mode allows only bytes as keys and values.

```python
from speedict import Rdict, Options

PATH_TO_ROCKSDB = str("path")

# open raw_mode, which allows only bytes
db = Rdict(path=PATH_TO_ROCKSDB, options=Options(raw_mode=True))

db[b'a'] = b'a'
db[b'b'] = b'b'
db[b'c'] = b'c'
db[b'd'] = b'd'

for k, v in db.items():
    print(f"{k} -> {v}")

# close and delete
db.close()
Rdict.destroy(PATH_TO_ROCKSDB)
```

## New Feature Since v0.3.3

Loading Options from RocksDict Path.

### Load Options and add A New ColumnFamily
```python
from speedict import Options, Rdict
path = str("./rocksdict_path")

opts, cols = Options.load_latest(path)
opts.create_missing_column_families(True)
cols["bytes"] = Options()
self.test_dict = Rdict(path, options=opts, column_families=cols)
```

### Reopening RocksDB Reads DB Options Automatically

```python
import shutil

from speedict import Rdict, Options, SliceTransform, PlainTableFactoryOptions
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
    # set to plain-table
    opt.set_plain_table_factory(PlainTableFactoryOptions())
    return opt


# create DB
db = Rdict("./some_path", db_options())
db[0] = 1
db.close()

# automatic reloading all options on reopening
db = Rdict("./some_path")
assert db[0] == 1

# destroy
db.close()
Rdict.destroy("./some_path")
```

## More Examples on BatchWrite, SstFileWrite, Snapshot, RocksDB Options, and etc.

Go to [example](https://github.com/Congyuwang/RocksDict/tree/main/examples) folder.

## Limitations

Currently, do not support merge operation and custom comparator.

## Full Documentation

See [speedict documentation](https://congyuwang.github.io/RocksDict/speedict.html).
