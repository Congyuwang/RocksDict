from .rocksdict import *
from .rocksdict import RdictInner as _Rdict
from .rocksdict import Pickle as _Pickle
import pickle as _pkl
from typing import Union, List, Any, Tuple, Iterator

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


class RdictItems(Iterator[Tuple[Union[str, int, float, bytes], Any]]):
    def __init__(self, d: _Rdict):
        r_opt = ReadOptions()
        r_opt.set_total_order_seek(True)
        self.inner = d.iter(r_opt)
        self.inner.seek_to_first()

    def __iter__(self):
        return self

    def __next__(self) -> Tuple[Union[str, int, float, bytes], Any]:
        if self.inner.valid():
            k = self.inner.key()
            v = self.inner.value()
            self.inner.next()
            return k, v
        raise StopIteration


class RdictKeys(Iterator[Union[str, int, float, bytes]]):
    def __init__(self, d: _Rdict):
        r_opt = ReadOptions()
        r_opt.set_total_order_seek(True)
        self.inner = d.iter(r_opt)
        self.inner.seek_to_first()

    def __iter__(self):
        return self

    def __next__(self) -> Union[str, int, float, bytes]:
        if self.inner.valid():
            k = self.inner.key()
            self.inner.next()
            return k
        raise StopIteration


class RdictValues(Iterator[Any]):
    def __init__(self, d: _Rdict):
        r_opt = ReadOptions()
        r_opt.set_total_order_seek(True)
        self.inner = d.iter(r_opt)
        self.inner.seek_to_first()

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if self.inner.valid():
            v = self.inner.value()
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

    def __getitem__(self, key: Union[str, int, float, bytes, List[Union[str, int, float, bytes]]]) -> Any:
        value = self._inner[key]
        if type(value) is _Pickle:
            return _pkl.loads(value.data)
        return value

    def __setitem__(self, key: Union[str, int, float, bytes], value) -> None:
        value_type = type(value)
        if value_type is str or value_type is int or value_type is float or value_type is bytes:
            self._inner[key] = value
        else:
            self._inner[key] = _Pickle(_pkl.dumps(value))

    def __contains__(self, key: Union[str, int, float, bytes]) -> bool:
        return key in self._inner

    def __delitem__(self, key: Union[str, int, float, bytes]) -> None:
        del self._inner[key]

    def items(self) -> Iterator[Tuple[Union[str, int, float, bytes], Any]]:
        """Similar to dict.items().

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

        Returns: Iterator

        """
        return RdictItems(self._inner)

    def values(self) -> Iterator[Union[str, int, float, bytes]]:
        """Similar to dict.values().

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

        Returns: Iterator

        """
        return RdictValues(self._inner)

    def keys(self) -> Iterator[Any]:
        """Similar to dict.keys().

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

        Returns: Iterator

        """
        return RdictKeys(self._inner)

    def iter(self, read_opt: ReadOptions) -> RdictIter:
        """Iterator for iterating over keys and values.

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

        Returns: Iterator

        """
        return self._inner.iter(read_opt)

    def close(self) -> None:
        """Flush the database.

        Notes:
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
