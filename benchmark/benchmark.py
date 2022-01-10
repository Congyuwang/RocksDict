from .db_impl import *
from random import choices
import pytest

VALUE_1B = b'a'


def pos_cover_num_to_str(x: int, size: int) -> str:
    """Generate fixed size string number for uniqueness."""
    x = str(x)
    return f"{'0' * (size - len(x))}{x}"


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class Dataset:
    def __init__(self, num: int, keys_size: int = 16,
                 value_size: int = 100, batch_size: int = 100,
                 random_percent: float = 0.01):
        self.value = VALUE_1B * value_size
        self.keys = [pos_cover_num_to_str(i, keys_size) for i in range(num)]
        self.bytes_keys = [bytes(k, 'utf-8') for k in self.keys]
        self.batched_kv = [[(k, self.value) for k in b]
                           for b in chunks(self.keys, batch_size)]
        self.bytes_batched_kv = [[(k, self.value) for k in b]
                                 for b in chunks(self.bytes_keys, batch_size)]
        self.keys_len = len(self.keys)
        num_keys = int(self.keys_len * random_percent)
        self.random_selected_keys = choices(self.keys, k=num_keys)
        self.random_selected_bytes_keys = choices(self.bytes_keys, k=num_keys)


def db_factory(db_name: str):
    if db_name == "rocks_db":
        return RocksDB()
    elif db_name == "rocks_db_raw":
        return RocksDBRaw()
    elif db_name == "sqlite_db":
        return SqliteDB()
    elif db_name == "sqlite_db_raw":
        return SqliteDBRAW()
    elif db_name == "shelve_db":
        return ShelveDB()
    elif db_name == "dbm":
        return DBM()
    elif db_name == "py_vidar_db":
        return PyVidarDB()
    elif db_name == "semi_dbm":
        return SemiDBM()
    elif db_name == "cannon_db":
        return CannonDB()
    else:
        raise Exception("dn_name should be one of: rocks_db, rocks_db_raw, " +
                        "sqlite_db, sqlite_db_raw, shelve_db, dbm, py_vidar_db,"
                        " semi_dbm, cannon_db")


def sample_factory(num: int, k_size: int, v_size: int,
                   batch_size: int, random_percent: float):
    # num, v_size = 100000, 100
    # num, v_size = 1000, 100000
    return Dataset(num, keys_size=k_size,
                   value_size=v_size,
                   batch_size=batch_size,
                   random_percent=random_percent)


@pytest.fixture()
def num(pytestconfig):
    return int(pytestconfig.getoption("num"))


@pytest.fixture()
def k_size(pytestconfig):
    return int(pytestconfig.getoption("k_size"))


@pytest.fixture()
def v_size(pytestconfig):
    return int(pytestconfig.getoption("v_size"))


@pytest.fixture()
def name(pytestconfig):
    return pytestconfig.getoption('dbname')


@pytest.fixture()
def batch_size(pytestconfig):
    return int(pytestconfig.getoption('batch_size'))


@pytest.fixture()
def percent(pytestconfig):
    return float(pytestconfig.getoption('percent'))


def test_get_data_sample(num, k_size, v_size, batch_size, percent):
    global data_sample
    data_sample = sample_factory(num, k_size, v_size, batch_size, percent)


def insert(db: ADB):
    for key in data_sample.keys:
        db.insert(key, data_sample.value)


def insert_raw(db: ADB):
    for key in data_sample.bytes_keys:
        db.insert_raw(key, data_sample.value)


def get(db: ADB):
    for key in data_sample.keys:
        assert db.get(key) == data_sample.value


def get_raw(db: ADB):
    for key in data_sample.bytes_keys:
        assert db.get_raw(key) == data_sample.value


def random_get(db: ADB):
    for key in data_sample.random_selected_keys:
        assert db.get(key) == data_sample.value


def random_get_raw(db: ADB):
    for key in data_sample.random_selected_bytes_keys:
        assert db.get_raw(key) == data_sample.value


def batch_insert(db: ADB):
    for batch in data_sample.batched_kv:
        db.batch_insert(batch)


def batch_insert_raw(db: ADB):
    for batch in data_sample.bytes_batched_kv:
        db.batch_insert_raw(batch)


def multi_get(db: ADB):
    for batch in data_sample.batched_kv:
        expected = [data_sample.value] * len(batch)
        assert db.multi_get([k for k, _ in batch]) == expected


def multi_get_raw(db: ADB):
    for batch in data_sample.bytes_batched_kv:
        expected = [data_sample.value] * len(batch)
        assert db.multi_get_raw([k for k, _ in batch]) == expected


def delete(db: ADB):
    for key in data_sample.keys:
        db.delete(key)


def delete_raw(db: ADB):
    for key in data_sample.bytes_keys:
        db.delete_raw(key)


@pytest.fixture()
def db_before_after(name):
    db = db_factory(name)
    yield db
    db.destroy()


@pytest.fixture()
def db_insert_before_after(name):
    db = db_factory(name)
    try:
        insert(db)
    except NotImplementedError:
        db.destroy()
        pytest.skip("method unimplemented")
    yield db
    db.destroy()


@pytest.fixture()
def db_insert_raw_before_after(name):
    db = db_factory(name)
    try:
        insert_raw(db)
    except NotImplementedError:
        db.destroy()
        pytest.skip("method unimplemented")
    yield db
    db.destroy()


def test_fill_sequential(db_before_after, benchmark):
    try:
        benchmark(insert, db_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_fill_raw_sequential(db_before_after, benchmark):
    try:
        benchmark(insert_raw, db_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_read_hot(db_insert_before_after, benchmark):
    try:
        benchmark(random_get, db_insert_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_read_hot_raw(db_insert_raw_before_after, benchmark):
    try:
        benchmark(random_get_raw, db_insert_raw_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_read_sequential(db_insert_before_after, benchmark):
    try:
        benchmark(get, db_insert_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_read_sequential_raw(db_insert_raw_before_after, benchmark):
    try:
        benchmark(get_raw, db_insert_raw_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_delete_sequential(db_insert_before_after, benchmark):
    try:
        benchmark(delete, db_insert_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_delete_sequential_raw(db_insert_raw_before_after, benchmark):
    try:
        benchmark(delete_raw, db_insert_raw_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_fill_batch_sequential(db_before_after, benchmark):
    try:
        benchmark(batch_insert, db_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_fill_raw_batch_sequential(db_before_after, benchmark):
    try:
        benchmark(batch_insert_raw, db_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_get_batch_sequential(db_insert_before_after, benchmark):
    try:
        benchmark(multi_get, db_insert_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")


def test_get_raw_batch_sequential(db_insert_raw_before_after, benchmark):
    try:
        benchmark(multi_get_raw, db_insert_raw_before_after)
    except NotImplementedError:
        pytest.skip("method unimplemented")
