import unittest
from sys import getrefcount
from rocksdict import (
    AccessType,
    Rdict,
    Options,
    PlainTableFactoryOptions,
    SliceTransform,
    CuckooTableOptions,
    DbClosedError,
)
from random import randint, random, getrandbits
import os
import sys
from json import loads, dumps


TEST_INT_RANGE_UPPER = 999999


def randbytes(n):
    """Generate n random bytes."""
    return getrandbits(n * 8).to_bytes(n, "little")


def compare_dicts(test_case: unittest.TestCase, ref_dict: dict, test_dict: Rdict):
    # assert that the values are the same
    test_case.assertEqual({k: v for k, v in test_dict.items()}, ref_dict)


class TestGetDel(unittest.TestCase):
    test_dict = None
    opt = None
    path = "./test_get_pul_del"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.test_dict["a"] = "a"
        cls.test_dict[123] = 123

    def testGetItem(self):
        self.assertEqual(self.test_dict["a"], "a")
        self.assertEqual(self.test_dict[123], 123)
        self.assertIsNone(self.test_dict.get("b"))
        self.assertIsNone(self.test_dict.get(250))
        self.assertEqual(self.test_dict.get("b", "b"), "b")
        self.assertEqual(self.test_dict.get(250, 1324123), 1324123)
        self.assertRaises(KeyError, lambda: self.test_dict["b"])
        self.assertRaises(KeyError, lambda: self.test_dict[250])

    def testDelItem(self):
        # no exception raise when deleting non-existing key
        self.test_dict.__delitem__("b")
        self.test_dict.__delitem__(250)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy(cls.path, Options())


class TestGetDelCustomDumpsLoads(unittest.TestCase):
    test_dict = None
    opt = None
    path = "./test_get_pul_del_loads_dumps"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.test_dict.set_loads(lambda x: loads(x.decode("utf-8")))
        cls.test_dict.set_dumps(lambda x: bytes(dumps(x), "utf-8"))
        cls.test_dict["a"] = "a"
        cls.test_dict[123] = 123
        cls.test_dict["ok"] = ["o", "k"]

    def testGetItem(self):
        self.assertEqual(self.test_dict["a"], "a")
        self.assertEqual(self.test_dict[123], 123)
        self.assertEqual(self.test_dict["ok"], ["o", "k"])
        self.assertIsNone(self.test_dict.get("b"))
        self.assertIsNone(self.test_dict.get(250))
        self.assertEqual(self.test_dict.get("b", "b"), "b")
        self.assertEqual(self.test_dict.get(250, 1324123), 1324123)
        self.assertEqual(self.test_dict["ok"], ["o", "k"])
        self.assertRaises(KeyError, lambda: self.test_dict["b"])
        self.assertRaises(KeyError, lambda: self.test_dict[250])

    def testDelItem(self):
        # no exception raise when deleting non-existing key
        self.test_dict.__delitem__("b")
        self.test_dict.__delitem__(250)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy(cls.path, Options())


class TestIterBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None
    path = "./temp_iter_bytes"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.increase_parallelism(os.cpu_count())
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.ref_dict = dict()
        for i in range(100000):
            key = randbytes(10)
            value = randbytes(20)
            cls.test_dict[key] = value
            cls.ref_dict[key] = value
        keys_to_remove = list(
            set(randint(0, len(cls.ref_dict) - 1) for _ in range(50000))
        )
        keys = [k for k in cls.ref_dict.keys()]
        keys_to_remove = [keys[i] for i in keys_to_remove]
        for key in keys_to_remove:
            del cls.test_dict[key]
            del cls.ref_dict[key]

    def test_seek_forward_key(self):
        key = randbytes(10)
        ref_list = [k for k in self.ref_dict.keys() if k >= key]
        ref_list.sort()
        self.assertEqual([k for k in self.test_dict.keys(from_key=key)], ref_list)

    def test_seek_backward_key(self):
        key = randbytes(20)
        ref_list = [k for k in self.ref_dict.keys() if k <= key]
        ref_list.sort(reverse=True)
        self.assertEqual(
            [k for k in self.test_dict.keys(from_key=key, backwards=True)], ref_list
        )

    def test_may_exists(self):
        for k, v in self.ref_dict.items():
            may_exists, value = self.test_dict.key_may_exist(k, fetch=True)
            self.assertTrue(may_exists)
            if value is not None:
                self.assertEqual(v, value)

    def test_seek_forward(self):
        key = randbytes(20)
        self.assertEqual(
            {k: v for k, v in self.test_dict.items(from_key=key)},
            {k: v for k, v in self.ref_dict.items() if k >= key},
        )

    def test_seek_backward(self):
        key = randbytes(20)
        self.assertEqual(
            {k: v for k, v in self.test_dict.items(from_key=key, backwards=True)},
            {k: v for k, v in self.ref_dict.items() if k <= key},
        )

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy(cls.path, Options())


class TestIterInt(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None
    path = "./temp_iter_int"

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict(cls.path)
        cls.ref_dict = dict()
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            value = randint(0, TEST_INT_RANGE_UPPER - 1)
            cls.ref_dict[key] = value
            cls.test_dict[key] = value
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            if key in cls.ref_dict:
                del cls.ref_dict[key]
                del cls.test_dict[key]

    def test_seek_forward(self):
        self.assertEqual(
            {k: v for k, v in self.test_dict.items()},
            {k: v for k, v in self.ref_dict.items()},
        )

    def test_seek_backward(self):
        self.assertEqual(
            {k: v for k, v in self.test_dict.items(backwards=True)},
            {k: v for k, v in self.ref_dict.items()},
        )

    def test_seek_forward_key(self):
        key = randint(0, TEST_INT_RANGE_UPPER - 1)
        ref_list = [k for k in self.ref_dict.keys() if k >= key]
        ref_list.sort()
        self.assertEqual([k for k in self.test_dict.keys(from_key=key)], ref_list)

    def test_seek_backward_key(self):
        key = randint(0, TEST_INT_RANGE_UPPER - 1)
        ref_list = [k for k in self.ref_dict.keys() if k <= key]
        ref_list.sort(reverse=True)
        self.assertEqual(
            [k for k in self.test_dict.keys(from_key=key, backwards=True)], ref_list
        )

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy(cls.path, Options())


class TestInt(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None
    path = "./temp_int"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            value = randint(0, TEST_INT_RANGE_UPPER - 1)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key]

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_delete_range(self):
        to_delete = []
        for key in self.ref_dict:
            if key >= 99999:
                to_delete.append(key)
        for key in to_delete:
            del self.ref_dict[key]
        self.test_dict.delete_range(99999, 10000000)
        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_reopen(self):
        self.test_dict.close()

        self.assertRaises(DbClosedError, lambda: self.test_dict.get(1))

        test_dict = Rdict(self.path, self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(
            self.test_dict.multi_get(keys + ["no such key"] * 3),
            [self.ref_dict[k] for k in keys] + [None] * 3,
        )

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy(cls.path, cls.opt)


class TestBigInt(unittest.TestCase):
    test_dict = None
    opt = None
    path = "./temp_big_int"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.opt.set_plain_table_factory(PlainTableFactoryOptions())
        cls.opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        cls.test_dict = Rdict(cls.path, cls.opt)

    def test_big_int(self):
        key = 13456436145354564353464754615223435465543
        value = 3456321456543245657643253647543212425364342343564
        self.test_dict[key] = value
        self.assertTrue(key in self.test_dict)
        self.assertEqual(self.test_dict[key], value)
        self.test_dict[key] = True
        self.assertTrue(self.test_dict[key])
        self.test_dict[key] = False
        self.assertFalse(self.test_dict[key])
        del self.test_dict[key]
        self.assertFalse(key in self.test_dict)

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy(cls.path, cls.opt)


class TestFloat(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None
    path = "./temp_float"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.ref_dict = dict()

    def test_add_float(self):
        for i in range(10000):
            key = random()
            value = random()
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_delete_float(self):
        for i in range(5000):
            keys = [k for k in self.ref_dict.keys()]
            key = keys[randint(0, len(self.ref_dict) - 1)]
            del self.ref_dict[key]
            del self.test_dict[key]

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_reopen(self):
        self.test_dict.close()
        test_dict = Rdict(self.path, self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(
            self.test_dict.multi_get(keys + ["no such key"] * 3),
            [self.ref_dict[k] for k in keys] + [None] * 3,
        )

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy(cls.path, cls.opt)


class TestBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None
    path = "./temp_bytes"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        # for the moment do not use CuckooTable on windows
        if not sys.platform.startswith("win"):
            cls.opt.set_cuckoo_table_factory(CuckooTableOptions())
            cls.opt.set_allow_mmap_reads(True)
            cls.opt.set_allow_mmap_writes(True)
        cls.test_dict = Rdict(cls.path, cls.opt)
        cls.ref_dict = dict()

    def test_add_bytes(self):
        for i in range(10000):
            key = randbytes(10)
            value = randbytes(20)
            self.assertEqual(getrefcount(key), 2)
            self.assertEqual(getrefcount(value), 2)
            self.test_dict[key] = value
            # rdict does not increase ref_count
            self.assertEqual(getrefcount(key), 2)
            self.assertEqual(getrefcount(value), 2)
            self.ref_dict[key] = value
            self.assertEqual(getrefcount(key), 3)
            self.assertEqual(getrefcount(value), 3)

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_delete_bytes(self):
        for i in range(5000):
            keys = [k for k in self.ref_dict.keys()]
            key = keys[randint(0, len(self.ref_dict) - 1)]
            # key + ref_dict + keys + getrefcount -> 4
            self.assertEqual(getrefcount(key), 4)
            del self.test_dict[key]
            self.assertEqual(getrefcount(key), 4)
            del self.ref_dict[key]
            self.assertEqual(getrefcount(key), 3)

        compare_dicts(self, self.ref_dict, self.test_dict)

    def test_reopen(self):
        self.test_dict.close()
        test_dict = Rdict(self.path, self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(
            self.test_dict.multi_get(keys + ["no such key"] * 3),
            [self.ref_dict[k] for k in keys] + [None] * 3,
        )

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy(cls.path, cls.opt)


class TestString(unittest.TestCase):
    test_dict = None
    opt = None
    path = "./temp_string"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict(cls.path, cls.opt)

    def test_string(self):
        self.test_dict["Guangdong"] = "Shenzhen"
        self.test_dict["Sichuan"] = "Changsha"
        # overwrite
        self.test_dict["Sichuan"] = "Chengdu"
        self.test_dict["Beijing"] = "Beijing"
        del self.test_dict["Beijing"]

        # assertions
        self.assertNotIn("Beijing", self.test_dict)
        self.assertEqual(self.test_dict["Sichuan"], "Chengdu")
        self.assertEqual(self.test_dict["Guangdong"], "Shenzhen")

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path, cls.opt)


class TestColumnFamiliesDefaultOpts(unittest.TestCase):
    test_dict = None
    path = "./column_families"

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict(cls.path)

    def test_column_families(self):
        ds = self.test_dict.create_column_family(name="string")
        di = self.test_dict.create_column_family(name="integer")

        for i in range(1000):
            di[i] = i * i
            ds[str(i)] = str(i * i)

        self.test_dict["ok"] = True

        ds.close()
        di.close()
        self.test_dict.close()

        # reopen
        self.test_dict = Rdict(self.path)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        assert self.test_dict["ok"]
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        self.test_dict.close()

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path)


class TestColumnFamiliesDefaultOptsCreate(unittest.TestCase):
    cfs = None
    test_dict = None
    path = "./column_families_create"

    @classmethod
    def setUpClass(cls) -> None:
        cls.cfs = {"string": Options(), "integer": Options()}
        opt = Options()
        opt.create_missing_column_families(True)
        cls.test_dict = Rdict(cls.path, options=opt, column_families=cls.cfs)

    def test_column_families_create(self):
        ds = self.test_dict.get_column_family(name="string")
        di = self.test_dict.get_column_family(name="integer")

        for i in range(1000):
            di[i] = i * i
            ds[str(i)] = str(i * i)

        self.test_dict["ok"] = True

        ds.close()
        di.close()
        self.test_dict.close()

        # reopen
        self.test_dict = Rdict(self.path)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        assert self.test_dict["ok"]
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        self.test_dict.close()

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path)


class TestColumnFamiliesCustomOpts(unittest.TestCase):
    cfs = None
    test_dict = None
    path = "./column_families_custom_options"

    @classmethod
    def setUpClass(cls) -> None:
        plain_opts = Options()
        plain_opts.create_missing_column_families(True)
        plain_opts.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        plain_opts.set_plain_table_factory(PlainTableFactoryOptions())
        cls.cfs = {"string": Options(), "integer": plain_opts}
        cls.test_dict = Rdict(cls.path, options=plain_opts, column_families=cls.cfs)

    def test_column_families_custom_options_auto_reopen(self):
        ds = self.test_dict.get_column_family(name="string")
        di = self.test_dict.get_column_family(name="integer")

        for i in range(1000):
            di[i] = i * i
            ds[str(i)] = str(i * i)

        self.test_dict["ok"] = True

        ds.close()
        di.close()
        self.test_dict.close()

        # reopen
        self.test_dict = Rdict(self.path)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        assert self.test_dict["ok"]
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        self.test_dict.close()

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path)


class TestColumnFamiliesCustomOptionsCreate(unittest.TestCase):
    cfs = None
    test_dict = None
    plain_opts = None
    path = "./column_families_custom_options_create"

    @classmethod
    def setUpClass(cls) -> None:
        cls.plain_opts = Options()
        cls.plain_opts.create_missing_column_families(True)
        cls.plain_opts.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        cls.plain_opts.set_plain_table_factory(PlainTableFactoryOptions())
        cls.test_dict = Rdict(cls.path, options=cls.plain_opts, column_families=cls.cfs)

    def test_column_families_custom_options_auto_reopen(self):
        ds = self.test_dict.create_column_family(name="string")
        di = self.test_dict.create_column_family(
            name="integer", options=self.plain_opts
        )

        for i in range(1000):
            di[i] = i * i
            ds[str(i)] = str(i * i)

        self.test_dict["ok"] = True

        ds.close()
        di.close()
        self.test_dict.close()

        # reopen
        self.test_dict = Rdict(self.path)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        assert self.test_dict["ok"]
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        self.test_dict.close()

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path)


class TestColumnFamiliesCustomOptionsCreateReopenOverride(unittest.TestCase):
    cfs = None
    test_dict = None
    plain_opts = None
    path = "./column_families_custom_options_create_reopen_override"

    @classmethod
    def setUpClass(cls) -> None:
        cls.plain_opts = Options()
        cls.plain_opts.create_missing_column_families(True)
        cls.plain_opts.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        cls.plain_opts.set_plain_table_factory(PlainTableFactoryOptions())
        cls.test_dict = Rdict(cls.path, options=cls.plain_opts)

    def test_column_families_custom_options_auto_reopen_override(self):
        ds = self.test_dict.create_column_family(name="string")
        di = self.test_dict.create_column_family(
            name="integer", options=self.plain_opts
        )

        for i in range(1000):
            di[i] = i * i
            ds[str(i)] = str(i * i)

        self.test_dict["ok"] = True

        ds.close()
        di.close()
        self.test_dict.close()

        # reopen
        old_opts, old_cols = Options.load_latest(self.path)
        old_opts.create_missing_column_families(True)
        old_cols["bytes"] = self.plain_opts
        self.test_dict = Rdict(self.path, options=old_opts, column_families=old_cols)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        db = self.test_dict.get_column_family("bytes")
        db[b"great"] = b"hello world"
        assert self.test_dict["ok"]
        assert db[b"great"] == b"hello world"
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        db.close()
        self.test_dict.close()

        # reopen again auto read config
        self.test_dict = Rdict(self.path)
        ds = self.test_dict.get_column_family("string")
        di = self.test_dict.get_column_family("integer")
        db = self.test_dict.get_column_family("bytes")
        db[b"great"] = b"hello world"
        assert self.test_dict["ok"]
        assert db[b"great"] == b"hello world"
        compare_dicts(self, {i: i**2 for i in range(1000)}, di)
        compare_dicts(self, {str(i): str(i**2) for i in range(1000)}, ds)
        ds.close()
        di.close()
        db.close()
        self.test_dict.close()

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        Rdict.destroy(cls.path)


class TestIntWithSecondary(unittest.TestCase):
    test_dict = None
    ref_dict = None
    secondary_dict = None
    opt = None
    path = "./temp_int_with_secondary"
    secondary_path = "./temp_int_with_secondary.secondary"

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict(cls.path, cls.opt)

        cls.secondary_dict = Rdict(
            cls.path,
            options=cls.opt,
            access_type=AccessType.secondary(cls.secondary_path),
        )

        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            value = randint(0, TEST_INT_RANGE_UPPER - 1)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        self.test_dict.flush(True)
        self.secondary_dict.try_catch_up_with_primary()
        compare_dicts(self, self.ref_dict, self.secondary_dict)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key]

        self.test_dict.flush(True)
        self.secondary_dict.try_catch_up_with_primary()
        compare_dicts(self, self.ref_dict, self.secondary_dict)

    def test_delete_range(self):
        to_delete = []
        for key in self.ref_dict:
            if key >= 99999:
                to_delete.append(key)
        for key in to_delete:
            del self.ref_dict[key]
        self.test_dict.delete_range(99999, 10000000)

        self.test_dict.flush(True)
        self.secondary_dict.try_catch_up_with_primary()
        compare_dicts(self, self.ref_dict, self.secondary_dict)

    def test_reopen(self):
        self.secondary_dict.close()

        self.assertRaises(DbClosedError, lambda: self.secondary_dict.get(1))

        self.secondary_dict = Rdict(
            self.path,
            options=self.opt,
            access_type=AccessType.secondary(self.secondary_path),
        )
        compare_dicts(self, self.ref_dict, self.secondary_dict)

    @classmethod
    def tearDownClass(cls):
        del cls.test_dict
        del cls.secondary_dict
        Rdict.destroy(cls.path, cls.opt)
        Rdict.destroy(cls.secondary_path, cls.opt)


if __name__ == "__main__":
    unittest.main()
