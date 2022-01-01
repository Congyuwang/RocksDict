import unittest
from sys import getrefcount
from rocksdict import Rdict, Options, PlainTableFactoryOptions, SliceTransform, CuckooTableOptions
from random import randint, random, getrandbits
import os
import sys


TEST_INT_RANGE_UPPER = 999999


def randbytes(n):
    """Generate n random bytes."""
    return getrandbits(n * 8).to_bytes(n, 'little')


def compare_dicts(test_case: unittest.TestCase,
                  ref_dict: dict,
                  test_dict: Rdict):
    # assert that the values are the same
    test_case.assertEqual({k: v for k, v in test_dict.items()}, ref_dict)


class TestIterBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.increase_parallelism(os.cpu_count())
        cls.test_dict = Rdict("./temp_iter_bytes", cls.opt)
        cls.ref_dict = dict()
        for i in range(100000):
            key = randbytes(10)
            value = randbytes(20)
            cls.test_dict[key] = value
            cls.ref_dict[key] = value
        keys_to_remove = list(set(randint(0, len(cls.ref_dict) - 1) for _ in range(50000)))
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
        self.assertEqual([k for k in reversed(self.test_dict.keys(from_key=key))], ref_list)

    def test_seek_forward(self):
        key = randbytes(20)
        self.assertEqual({k: v for k, v in self.test_dict.items(from_key=key)},
                         {k: v for k, v in self.ref_dict.items() if k >= key})

    def test_seek_backward(self):
        key = randbytes(20)
        self.assertEqual({k: v for k, v in reversed(self.test_dict.items(from_key=key))},
                         {k: v for k, v in self.ref_dict.items() if k <= key})

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy("./temp_iter_bytes", Options())


class TestIterInt(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Rdict("./temp_iter_int")
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

    def test_seek_forward_key(self):
        key = randint(0, TEST_INT_RANGE_UPPER - 1)
        ref_list = [k for k in self.ref_dict.keys() if k >= key]
        test_list = set(key for key in self.test_dict.keys(from_key=key))
        # this is due to characteristic of VarInt encoding
        self.assertTrue(all(key in test_list for key in ref_list))

    def test_seek_backward_key(self):
        key = randint(0, TEST_INT_RANGE_UPPER - 1)
        ref_list = [k for k in self.ref_dict.keys() if k <= key]
        test_list = [key for key in reversed(self.test_dict.keys(from_key=key))]
        # this is due to characteristic of VarInt encoding
        self.assertTrue(all(key in ref_list for key in test_list))

    def test_seek_forward(self):
        self.assertEqual({k: v for k, v in self.test_dict.items()},
                         {k: v for k, v in self.ref_dict.items()})

    def test_seek_backward(self):
        self.assertEqual({k: v for k, v in reversed(self.test_dict.items())},
                         {k: v for k, v in self.ref_dict.items()})

    @classmethod
    def tearDownClass(cls):
        cls.test_dict.close()
        Rdict.destroy("./temp_iter_int", Options())


class TestInt(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.opt.set_plain_table_factory(PlainTableFactoryOptions())
        cls.opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        cls.test_dict = Rdict("./temp_int", cls.opt)
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

    def test_reopen(self):
        self.test_dict.close()
        test_dict = Rdict("./temp_int", self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict[keys + ["no such key"] * 3], [self.ref_dict[k] for k in keys] + [None] * 3)

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy("./temp_int", cls.opt)


class TestBigInt(unittest.TestCase):
    test_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.opt.set_plain_table_factory(PlainTableFactoryOptions())
        cls.opt.set_prefix_extractor(SliceTransform.create_max_len_prefix(8))
        cls.test_dict = Rdict("./temp_big_int", cls.opt)

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
        Rdict.destroy("./temp_big_int", cls.opt)


class TestFloat(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict("./temp_float", cls.opt)
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
        test_dict = Rdict("./temp_float", self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict[keys + ["no such key"] * 3], [self.ref_dict[k] for k in keys] + [None] * 3)

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy("./temp_float", cls.opt)


class TestBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        # for the moment do not use CuckooTable on windows
        if not sys.platform.startswith('win'):
            cls.opt.set_cuckoo_table_factory(CuckooTableOptions())
            cls.opt.set_allow_mmap_reads(True)
            cls.opt.set_allow_mmap_writes(True)
        cls.test_dict = Rdict("./temp_bytes", cls.opt)
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
        test_dict = Rdict("./temp_bytes", self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict[keys + ["no such key"] * 3], [self.ref_dict[k] for k in keys] + [None] * 3)

    @classmethod
    def tearDownClass(cls):
        Rdict.destroy("./temp_bytes", cls.opt)


class TestString(unittest.TestCase):
    test_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict("./temp_string", cls.opt)

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
        Rdict.destroy("./temp_string", cls.opt)


if __name__ == '__main__':
    unittest.main()
