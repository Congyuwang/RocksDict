from .rocksdict import *
import pickle as _pkl
from typing import Union


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

    def set_write_options(self, write_opt: WriteOptions):
        """Configure Write Options."""
        self._inner.set_write_options(write_opt)

    def set_flush_options(self, flush_opt: FlushOptions):
        """Configure Flush Options."""
        self._inner.set_flush_options(flush_opt)

    def set_read_options(self, read_opt: ReadOptions):
        """Configure Read Options."""
        self._inner.set_read_options(read_opt)

    def __getitem__(self, key: Union[str, int, float, bytes]):
        if type(key) is _Pickle:
            return _pkl.loads(self._inner[key].data)
        return self._inner[key]

    def __setitem__(self, key: Union[str, int, float, bytes], value):
        value_type = type(value)
        if value_type is str or value_type is int or value_type is float or value_type is bytes:
            self._inner[key] = value
        else:
            print("we are here")
            self._inner[key] = _Pickle(_pkl.dumps(value))

    def __contains__(self, key: Union[str, int, float, bytes]):
        return key in self._inner

    def __delitem__(self, key: Union[str, int, float, bytes]):
        del self._inner[key]

    def close(self):
        """Flush the database.

        Notes:
            The database would not be usable after `close()` is called.
            Calling method after `close()` will throw exception.

        """
        self._inner.close()

    def destroy(self, options: Options):
        """Delete the database.

        Args:
            options (rocksdict.Options): Rocksdb options object

        """
        self._inner.destroy(options)


class Mdict:
    """An in-memory equivalent of Rdict."""

    def __init__(self):
        self._inner = _Mdict()

    def __getitem__(self, key: Union[str, int, float, bytes]):
        if type(key) is _Pickle:
            return _pkl.loads(self._inner[key].data)
        return self._inner[key]

    def __setitem__(self, key: Union[str, int, float, bytes], value):
        value_type = type(value)
        if value_type is str or value_type is int or value_type is float or value_type is bytes:
            self._inner[key] = value
        else:
            print("we are here")
            self._inner[key] = _Pickle(_pkl.dumps(value))

    def __contains__(self, key: Union[str, int, float, bytes]):
        return key in self._inner

    def __delitem__(self, key: Union[str, int, float, bytes]):
        del self._inner[key]

    def __len__(self):
        return len(self._inner)
