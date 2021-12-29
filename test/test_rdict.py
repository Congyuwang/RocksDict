import unittest
from sys import getrefcount
from rocksdict import Rdict, Options
from random import randint, random, getrandbits


TEST_INT_RANGE_UPPER = 999999


def randbytes(n):
    """Generate n random bytes."""
    return getrandbits(n * 8).to_bytes(n, 'little')


def compare_int_dicts(test_case: unittest.TestCase,
                      ref_dict: dict,
                      test_dict: Rdict,
                      lower: int,
                      upper: int):
    # assert that the keys are the same
    keys_ref = list(ref_dict.keys())
    keys_ref.sort()
    keys_test = [k for k in range(lower, upper) if k in test_dict]
    test_case.assertEqual(keys_ref, keys_test)

    # assert that the values are the same
    for k, v in ref_dict.items():
        test_case.assertTrue(k in test_dict)
        test_case.assertEqual(test_dict[k], v)


def compare_dicts(test_case: unittest.TestCase,
                  ref_dict: dict,
                  test_dict: Rdict):
    # assert that the values are the same
    for k, v in ref_dict.items():
        test_case.assertTrue(k in test_dict)
        test_case.assertEqual(test_dict[k], v)


class TestInt(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
        cls.test_dict = Rdict("./temp_int", cls.opt)
        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            value = randint(0, TEST_INT_RANGE_UPPER - 1)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_int_dicts(self, self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER - 1)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key]

        compare_int_dicts(self, self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_reopen(self):
        self.test_dict.close()
        self.test_dict = None
        opt = Options()
        opt.create_if_missing(True)
        test_dict = Rdict("./temp_int", opt)
        compare_int_dicts(self, self.ref_dict, test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict.get_batch(keys), [self.ref_dict[k] for k in keys])

    @classmethod
    def tearDownClass(cls):
        Rdict("./temp_int", cls.opt).destroy(cls.opt)


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
        self.test_dict = None
        test_dict = Rdict("./temp_float", self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict.get_batch(keys), [self.ref_dict[k] for k in keys])

    @classmethod
    def tearDownClass(cls):
        Rdict("./temp_float", cls.opt).destroy(cls.opt)


class TestBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None
    opt = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.opt = Options()
        cls.opt.create_if_missing(True)
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
        self.test_dict = None
        opt = Options()
        opt.create_if_missing(True)
        test_dict = Rdict("./temp_bytes", self.opt)
        compare_dicts(self, self.ref_dict, test_dict)

    def test_get_batch(self):
        keys = list(self.ref_dict.keys())[:100]
        self.assertEqual(self.test_dict.get_batch(keys), [self.ref_dict[k] for k in keys])

    @classmethod
    def tearDownClass(cls):
        Rdict("./temp_bytes", cls.opt).destroy(cls.opt)


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
        cls.test_dict.destroy(cls.opt)


if __name__ == '__main__':
    unittest.main()
