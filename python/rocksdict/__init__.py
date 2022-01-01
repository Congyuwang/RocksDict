from .rocksdict import *
from .rocksdict import RdictInner as _Rdict
from .rocksdict import Pickle as _Pickle
import pickle as _pkl
from typing import Union, List, Any, Tuple, Reversible

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


class Rdict:
    """
    A persistent on-disk key value storage.
    """

    def __init__(self, path: str, options: Options = Options()):
        """Create a new database or open an existing one.

        Args:
            path: path to the database
            options: Options object
        """
        self._inner = _Rdict(path, options)

    def set_write_options(self, write_opt: WriteOptions) -> None:
        """Configure Write Options."""
        self._inner.set_write_options(write_opt)

    def set_flush_options(self, flush_opt: FlushOptions) -> None:
        """Configure Flush Options."""
        self._inner.set_flush_options(flush_opt)

    def set_read_options(self, read_opt: ReadOptions) -> None:
        """Configure Read Options."""
        self._inner.set_read_options(read_opt)

    def __getitem__(self, key: Union[str, int, float, bytes, bool, List[Union[str, int, float, bytes, bool]]]) -> Any:
        value = self._inner[key]
        if type(value) is _Pickle:
            return _pkl.loads(value.data)
        if type(key) is list:
            return [_pkl.loads(v.data) if type(v) is _Pickle else v for v in value]
        return value

    def __setitem__(self, key: Union[str, int, float, bytes, bool], value) -> None:
        v_type = type(value)
        if v_type is str or v_type is int or v_type is float or v_type is bytes or v_type is bool:
            self._inner[key] = value
        else:
            self._inner[key] = _Pickle(_pkl.dumps(value))

    def __contains__(self, key: Union[str, int, float, bytes, bool]) -> bool:
        return key in self._inner

    def __delitem__(self, key: Union[str, int, float, bytes, bool]) -> None:
        del self._inner[key]

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
        return RdictItems(self._inner.iter(read_opt), backward, from_key)

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
        return RdictValues(self._inner.iter(read_opt), backward, from_key)

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
        return RdictKeys(self._inner.iter(read_opt), backward, from_key)

    def iter(self, read_opt: ReadOptions = ReadOptions()) -> RdictIter:
        """Reversible for iterating over keys and values.

        Examples:
            ::

            ```python
            from rocksdict import Rdict, Options, ReadOptions

            path = "_path_for_rocksdb_storage5"
            db = Rdict(path, Options())

            for i in range(50):
                db[i] = i ** 2

            iter = db.iter(ReadOptions())

            iter.seek_to_first()

            j = 0
            while iter.valid():
                assert iter.key() == j
                assert iter.value() == j ** 2
                print(f"{iter.key()} {iter.value()}")
                iter.next()
                j += 1

            iter.seek_to_first();
            assert iter.key() == 0
            assert iter.value() == 0
            print(f"{iter.key()} {iter.value()}")

            iter.seek(25)
            assert iter.key() == 25
            assert iter.value() == 625
            print(f"{iter.key()} {iter.value()}")

            del iter, db
            Rdict.destroy(path, Options())
            ```

        Args:
            read_opt: ReadOptions

        Returns: Reversible

        """
        return self._inner.iter(read_opt)

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
        self._inner.close()

    @staticmethod
    def destroy(path: str, options: Options) -> None:
        """Delete the database.

        Args:
            path (str): path to this database
            options (rocksdict.Options): Rocksdb options object

        """
        _Rdict.destroy(path, options)
