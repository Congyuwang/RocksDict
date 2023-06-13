"""
Actual Implementation of DataBases are placed here.

"""
import os
from pathlib import Path
from typing import Any, List, Tuple

import semidbm
from sqlitedict import SqliteDict
import shelve
import dbm
import pyvidardb
import cannondb
from speedict import Rdict, Options, WriteBatch
from .abstract_db import ADB
import shutil


class RocksDB(ADB):

    def __init__(self, path='./my_db_rocksdb/'):
        self.db = Rdict(path=path)

    def get(self, key: Any) -> Any:
        return self.db[key]

    def multi_get(self, key: List) -> List:
        return self.db[key]

    def insert(self, key: Any, value: Any) -> None:
        self.db[key] = value

    def batch_insert(self, kv_list: List[Tuple[Any, Any]]) -> None:
        wb = WriteBatch()
        for k, v in kv_list:
            wb[k] = v
        self.db.write(wb)

    def delete(self, key: Any) -> None:
        del self.db[key]

    def delete_range(self, start: Any, end: Any) -> None:
        self.db.delete_range(begin=start, end=end)

    def contains(self, key: Any) -> bool:
        return key in self.db

    def destroy(self) -> None:
        path = self.db.path()
        self.db.close()
        Rdict.destroy(path)


class RocksDBRaw(ADB):

    def __init__(self, path='./my_db_rocksdb_raw/'):
        self.db = Rdict(path=path, options=Options(raw_mode=True))

    def get_raw(self, key: bytes) -> bytes:
        return self.db[key]

    def multi_get_raw(self, key: List) -> List:
        return self.db[key]

    def insert_raw(self, key: bytes, value: bytes) -> None:
        self.db[key] = value

    def batch_insert_raw(self, kv_list: List[Tuple[bytes, bytes]]) -> None:
        wb = WriteBatch(raw_mode=True)
        for k, v in kv_list:
            wb[k] = v
        self.db.write(wb)

    def delete_raw(self, key: bytes) -> None:
        del self.db[key]

    def delete_range_raw(self, start: bytes, end: bytes) -> None:
        self.db.delete_range(begin=start, end=end)

    def contains_raw(self, key: bytes) -> bool:
        return key in self.db

    def destroy(self) -> None:
        path = self.db.path()
        self.db.close()
        Rdict.destroy(path)


class SqliteDB(ADB):
    def __init__(self, path='./my_db_sqlite/', name='my_db.sqlite',
                 commit_every: int = 1000):
        os.makedirs(path)
        self.path = path
        self.db = SqliteDict(Path(path) / name, autocommit=False, flag="c")
        self.commit_every = commit_every
        self.counter = 0

    def insert(self, key: Any, value: Any) -> None:
        self.db[key] = value
        self.counter += 1
        if self.counter % self.commit_every == 0:
            self.db.commit(blocking=True)

    def get(self, key: Any) -> Any:
        return self.db[key]

    def batch_insert(self, kv_list: List[Tuple[Any, Any]]) -> None:
        wb = {}
        for k, v in kv_list:
            wb[k] = v
        self.db.update(wb)
        self.db.commit(blocking=True)

    def delete(self, key: Any) -> None:
        del self.db[key]
        if self.counter % self.commit_every == 0:
            self.db.commit(blocking=True)

    def contains(self, key: Any) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)


class SqliteDBRAW(ADB):
    def __init__(self, path='./my_db_sqlite_raw/', name='my_db.sqlite',
                 commit_every: int = 1000):
        os.makedirs(path)
        self.path = path
        self.db = SqliteDict(Path(path) / name, autocommit=False, flag="c")
        self.commit_every = commit_every
        self.counter = 0

    def insert_raw(self, key: bytes, value: bytes) -> None:
        self.db[key] = value
        if self.counter % self.commit_every == 0:
            self.db.commit(blocking=True)

    def get_raw(self, key: Any) -> Any:
        return self.db[key]

    def batch_insert_raw(self, kv_list: List[Tuple[bytes, bytes]]) -> None:
        wb = {}
        for k, v in kv_list:
            wb[k] = v
        self.db.update(wb)

    def delete_raw(self, key: bytes) -> None:
        del self.db[key]
        if self.counter % self.commit_every == 0:
            self.db.commit(blocking=True)

    def contains_raw(self, key: bytes) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)


class ShelveDB(ADB):
    def __init__(self, path='./my_db_shelf/', name='my_db'):
        os.makedirs(path)
        self.path = path
        self.db = shelve.open(os.path.join(path, name))

    def insert(self, key: Any, value: Any) -> None:
        self.db[key] = value

    def get(self, key: Any) -> Any:
        return self.db[key]

    def delete(self, key: Any) -> None:
        del self.db[key]

    def contains(self, key: Any) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)


class DBM(ADB):
    def __init__(self, path='./my_db_dbm/', name='my_db'):
        os.makedirs(path)
        self.path = path
        self.db = dbm.open(os.path.join(path, name), "c")

    def insert_raw(self, key: bytes, value: bytes) -> None:
        self.db[key] = value

    def get_raw(self, key: bytes) -> bytes:
        return self.db[key]

    def delete_raw(self, key: bytes) -> None:
        del self.db[key]

    def contains_raw(self, key: bytes) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)


class PyVidarDB(ADB):
    def __init__(self, path='./my_db_vidardb/', name='my_db'):
        os.makedirs(path)
        self.path = path
        self.db = pyvidardb.DB()
        self.db.open(os.path.join(path, name), pyvidardb.Options())

    def insert_raw(self, key: bytes, value: bytes) -> None:
        self.db.put(key, value)

    def get_raw(self, key: bytes) -> bytes:
        return self.db.get(key)[0]

    def delete_raw(self, key: bytes) -> None:
        self.db.delete(key)

    def contains_raw(self, key: bytes) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)


class SemiDBM(ADB):
    def __init__(self, path='./my_db_semi/', name='my_db'):
        os.makedirs(path)
        self.path = path
        self.db = semidbm.open(os.path.join(path, name), 'c')

    def insert_raw(self, key: bytes, value: bytes) -> None:
        self.db[key] = value

    def get_raw(self, key: bytes) -> bytes:
        return self.db[key]

    def delete_raw(self, key: bytes) -> None:
        del self.db[key]

    def contains_raw(self, key: bytes) -> bool:
        return key in self.db

    def destroy(self) -> None:
        self.db.close()
        shutil.rmtree(self.path)
