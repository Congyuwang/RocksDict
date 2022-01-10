# RocksDict

**Key-value storage supporting any python object**

![CI](https://github.com/Congyuwang/RocksDict/actions/workflows/CI.yml/badge.svg)
![PyPI](https://img.shields.io/pypi/dm/rocksdict)
![PyPI](https://img.shields.io/pypi/wheel/rocksdict)
[![Support python versions](https://img.shields.io/pypi/pyversions/rocksdict.svg)](https://pypi.org/project/rocksdict/)

## Introduction

This library has two purposes.

1. As an on-disk key-value storage solution for Python.
2. As a RocksDB interface.

These two purposes operate in different modes:

- **Default mode**, which allows storing `int`, `float`, 
`bool`, `str`, `bytes`, and other python objects.

- **Raw mode** (`options=Options(raw_mode=True)`),
which allows storing only `bytes`.

## Installation

Wheels available, just `pip install rocksdict`.

## Examples

### A minimal example

```python
from rocksdict import Rdict
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
from rocksdict import Rdict, Options

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

## More Examples on BatchWrite, SstFileWrite, Snapshot, RocksDB Options, and etc.

Go to [example](https://github.com/Congyuwang/RocksDict/tree/main/examples) folder.

## A Simple Benchmark

Compared to [PyVidarDB](https://github.com/vidardb/PyVidarDB) and [semidbm](https://github.com/jamesls/semidbm),
which are all cross-platform key-value storage solutions.

### Small Value: 100 byte value

#### sequetial insertion
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/insert_sequential(num_keys%3D10000-%20ksize%3D16-%20vsize%3D100).png)
#### sequential read
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/read_sequential(num_keys%3D10000-%20ksize%3D16-%20vsize%3D100).png)
#### random read
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/random_read(num_keys%3D10000-%20ksize%3D16-%20vsize%3D100).png)

### Large Value: 100 kb value

#### sequetial insertion
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/insert_sequential(num_keys%3D1000-%20ksize%3D16-%20vsize%3D100000).png)
#### sequential read
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/read_sequential(num_keys%3D1000-%20ksize%3D16-%20vsize%3D100000).png)
#### random read
![image](https://github.com/Congyuwang/RocksDict/blob/main/benchmark/bench_plot/random_read(num_keys%3D1000-%20ksize%3D16-%20vsize%3D100000).png)

## Limitations

Currently, do not support merge operation and custom comparator.

## Full Documentation

See [rocksdict documentation](https://congyuwang.github.io/RocksDict/rocksdict.html).

## Contribution

This project is still in an early stage of development. People are welcome 
to add tests, benchmarks and new features.
