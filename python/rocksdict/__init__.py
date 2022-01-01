from .rocksdict import *
from typing import Union, Any, Tuple, Reversible

__all__ = ["DataBlockIndexType",
           "BlockBasedIndexType",
           "BlockBasedOptions",
           "Cache",
           "CuckooTableOptions",
           "DBCompactionStyle",
           "DBCompressionType",
           "DBPath",
           "DBRecoveryMode",
           "Env",
           "FifoCompactOptions",
           "FlushOptions",
           "MemtableFactory",
           "Options",
           "PlainTableFactoryOptions",
           "ReadOptions",
           "SliceTransform",
           "UniversalCompactOptions",
           "UniversalCompactionStopStyle",
           "WriteOptions",
           "Rdict",
           "RdictIter",
           "RdictItems",
           "RdictKeys",
           "RdictValues"]


class RdictItems(Reversible[Tuple[Union[str, int, float, bytes], Any]]):
    def __init__(self, inner: RdictIter,
                 backward: bool = False,
                 from_key: Union[str, int, float, bytes, bool, None] = None):
        """A more convenient interface for iterating through Rdict.

        Args:
            inner: the inner Rdict
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
        """
        self.inner = inner
        self.from_key = from_key
        if from_key:
            self.backward = backward
            if backward:
                self.inner.seek_for_prev(from_key)
            else:
                self.inner.seek(from_key)
        elif backward:
            self.inner.seek_to_last()
            self.backward = True
        else:
            self.inner.seek_to_first()
            self.backward = False

    def __iter__(self):
        return self

    def __reversed__(self):
        return RdictItems(self.inner, not self.backward, self.from_key)

    def __next__(self) -> Tuple[Union[str, int, float, bytes], Any]:
        if self.inner.valid():
            k = self.inner.key()
            v = self.inner.value()
            if self.backward:
                self.inner.prev()
            else:
                self.inner.next()
            return k, v
        raise StopIteration


class RdictKeys(Reversible[Union[str, int, float, bytes]]):
    def __init__(self, inner: RdictIter,
                 backward: bool = False,
                 from_key: Union[str, int, float, bytes, bool, None] = None):
        """A more convenient interface for iterating through Rdict.

        Args:
            inner: the inner Rdict
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
        """
        self.inner = inner
        self.from_key = from_key
        if from_key:
            self.backward = backward
            if backward:
                self.inner.seek_for_prev(from_key)
            else:
                self.inner.seek(from_key)
        elif backward:
            self.inner.seek_to_last()
            self.backward = True
        else:
            self.inner.seek_to_first()
            self.backward = False

    def __iter__(self):
        return self

    def __reversed__(self):
        return RdictKeys(self.inner, not self.backward, self.from_key)

    def __next__(self) -> Union[str, int, float, bytes]:
        if self.inner.valid():
            k = self.inner.key()
            if self.backward:
                self.inner.prev()
            else:
                self.inner.next()
            return k
        raise StopIteration


class RdictValues(Reversible[Any]):
    def __init__(self, inner: RdictIter,
                 backward: bool = False,
                 from_key: Union[str, int, float, bytes, bool, None] = None):
        """A more convenient interface for iterating through Rdict.

        Args:
            inner: the inner Rdict
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
        """
        self.inner = inner
        self.from_key = from_key
        if from_key:
            self.backward = backward
            if backward:
                self.inner.seek_for_prev(from_key)
            else:
                self.inner.seek(from_key)
        elif backward:
            self.inner.seek_to_last()
            self.backward = True
        else:
            self.inner.seek_to_first()
            self.backward = False

    def __iter__(self):
        return self

    def __reversed__(self):
        return RdictValues(self.inner, not self.backward, self.from_key)

    def __next__(self) -> Any:
        if self.inner.valid():
            v = self.inner.value()
            if self.backward:
                self.inner.prev()
            else:
                self.inner.next()
            return v
        raise StopIteration


class Rdict(RdictInner):
    """
    ## Abstract

    This package enables users to store, query, and delete
    a large number of key-value pairs on disk.

    This is especially useful when the data cannot fit into RAM.
    If you have hundreds of GBs or many TBs of key-value data to store
    and query from, this is the package for you.

    ### Installation

    This packakge is built for MacOS (x86/arm), Windows 64/32, and Linux x86.
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

    Examples:
        ::

            ```python
            from rocksdict import Rdict, Options

            path = str("./test_dict")

            # create a Rdict with default options at `path`
            db = Rdict(path)

            # storing numbers
            db[1.0] = 1
            db[1] = 1.0
            # very big integer
            db["huge integer"] = 2343546543243564534233536434567543
            # boolean values
            db["good"] = True
            db["bad"] = False
            # bytes
            db["bytes"] = b"bytes"
            # store anything
            db["this is a list"] = [1, 2, 3]
            db["store a dict"] = {0: 1}
            # for example numpy array
            import numpy as np
            import pandas as pd
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
            del db
            Rdict.destroy(path, Options())
            ```

    Supported types:

    - key: `int, float, bool, str, bytes`
    - value: `int, float, bool, str, bytes` and anything that
        supports `pickle`.

    """

    def items(self,
              backward: bool = False,
              from_key: Union[str, int, float, bytes, bool, None] = None,
              read_opt: ReadOptions = ReadOptions()) -> Reversible[Tuple[Union[str, int, float, bytes], Any]]:
        """Similar to dict.items().

        Args:
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
            read_opt: read options, which can be used to set iterator boundaries

        Examples:
            ::

                ```python
                from rocksdict import Rdict, Options, ReadOptions

                path = "_path_for_rocksdb_storage5"
                db = Rdict(path, Options())

                for i in range(50):
                    db[i] = i ** 2

                count = 0
                for k, v in db.items():
                    assert k == count
                    assert v == k ** 2
                    count += 1

                del db
                Rdict.destroy(path, Options())
                ```

        Returns: Reversible

        """
        return RdictItems(self.iter(read_opt), backward, from_key)

    def values(self,
               backward: bool = False,
               from_key: Union[str, int, float, bytes, bool, None] = None,
               read_opt: ReadOptions = ReadOptions()) -> Reversible[Union[str, int, float, bytes]]:
        """Similar to dict.values().

        Args:
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
            read_opt: read options, which can be used to set iterator boundaries

        Examples:
            ::

                ```python
                from rocksdict import Rdict, Options, ReadOptions

                path = "_path_for_rocksdb_storage5"
                db = Rdict(path, Options())

                for i in range(50):
                    db[i] = i ** 2

                count = 0
                for v in db.values():
                    assert v == count ** 2
                    count += 1

                del db
                Rdict.destroy(path, Options())
                ```

        Returns: Reversible

        """
        return RdictValues(self.iter(read_opt), backward, from_key)

    def keys(self,
             read_opt: ReadOptions = ReadOptions(),
             backward: bool = False,
             from_key: Union[str, int, float, bytes, bool, None] = None) -> Reversible[Any]:
        """Similar to dict.keys().

        Args:
            backward: iteration direction, forward if `False`.
            from_key: iterate from key, first seek to this key
                or the nearest next key for iteration
                (depending on iteration direction).
            read_opt: read options, which can be used to set iterator boundaries

        Examples:
            ::

                ```python
                from rocksdict import Rdict, Options, ReadOptions

                path = "_path_for_rocksdb_storage5"
                db = Rdict(path, Options())

                for i in range(50):
                    db[i] = i ** 2

                count = 0
                for v in db.keys():
                    assert v == count
                    count += 1

                del db
                Rdict.destroy(path, Options())
                ```

        Returns: Reversible

        """
        return RdictKeys(self.iter(read_opt), backward, from_key)

    def set_write_options(self, write_opt: WriteOptions) -> None:
        """Configure Write Options."""
        super(Rdict, self).set_write_options(write_opt)

    def set_flush_options(self, flush_opt: FlushOptions) -> None:
        """Configure Flush Options."""
        super(Rdict, self).set_flush_options(flush_opt)

    def set_read_options(self, read_opt: ReadOptions) -> None:
        """Configure Read Options."""
        super(Rdict, self).set_read_options(read_opt)

    def close(self) -> None:
        """Flush memory to disk, and drop the database.

        Notes:
            Setting Rdict to `None` does not always immediately close
            the database depending on the garbage collector of python.
            Calling `close()` is a more reliable method to ensure
            that the database is correctly closed.

            The database would not be usable after `close()` is called.
            Calling method after `close()` will throw exception.

        """
        super(Rdict, self).close()

    @staticmethod
    def destroy(path: str, options: Options) -> None:
        """Delete the database.

        Args:
            path (str): path to this database
            options (rocksdict.Options): Rocksdb options object

        """
        RdictInner.destroy(path, options)
