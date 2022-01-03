# RocksDict

**Key-value storage supporting any python object**

![CI](https://github.com/Congyuwang/RocksDict/actions/workflows/CI.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/dm/rocksdict)
![PyPI](https://img.shields.io/pypi/wheel/rocksdict)
[![Support python versions](https://img.shields.io/pypi/pyversions/rocksdict.svg)](https://pypi.org/project/rocksdict/)

## Abstract

This package enables users to store, query, and delete
a large number of key-value pairs on disk.

This is especially useful when the data cannot fit into RAM.
If you have hundreds of GBs or many TBs of key-value data to store
and query from, this is the package for you.

### Installation

This package is built for macOS (x86/arm), Windows 64/32, and Linux x86.
It can be installed from pypi with `pip install rocksdict`.

## Plans

- [x] set, get, del
- [x] multi get
- [x] support string, float, int, bytes
- [x] support other python objects through pickle
- [x] support BigInt
- [x] compare BigInt by value size
- [x] keys, values, items iterator
- [x] options, read options, write options, all options
- [x] SstFileWriter and bulk ingest
- [x] column families
- [x] write batch
- [x] delete range
- [x] open as secondary, with-ttl, read-only
- [ ] Snapshot
- [ ] support merge

## Supported key-value types:

- key: `int, float, bool, str, bytes`
- value: `int, float, bool, str, bytes` and anything that supports `pickle`.

## Introduction

Below is a minimal example that shows how to do the following:

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

## Supported key value types

- key: `int, float, bool, str, bytes`
- value: `int, float, bool, str, bytes` and anything that supports `pickle`.

## Examples

For examples on column families, batch write, RocksDB options, SstFileWriter, delete_range, & etc,
go to [example](https://github.com/Congyuwang/RocksDict/tree/main/examples) folder.

## Rocksdb Options

Since the backend is implemented using rocksdb,
most of rocksdb options are supported.

## Limitations

Currently do not have good support for merge operation.

## Full Documentation

See [rocksdict documentation](https://congyuwang.github.io/RocksDict/rocksdict.html).

## Contribution

This project is still in an early stage of development. People are welcome 
to add tests, benchmarks and new features.
