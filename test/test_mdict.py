import unittest
from sys import getrefcount
from rocksdict import Mdict
from random import randint, random, randbytes


TEST_INT_RANGE_UPPER = 999999


def compare_int_dicts(test_case: unittest.TestCase,
                      ref_dict: dict,
                      test_dict: Mdict,
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
                  test_dict: Mdict):
    # assert that the values are the same
    for k, v in ref_dict.items():
        test_case.assertTrue(k in test_dict)
        test_case.assertEqual(test_dict[k], v)


class TestInt(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Mdict()
        cls.ref_dict = dict()

    def test_add_integer(self):
        for i in range(10000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            value = randint(0, TEST_INT_RANGE_UPPER)
            self.ref_dict[key] = value
            self.test_dict[key] = value

        compare_int_dicts(self, self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)

    def test_delete_integer(self):
        for i in range(5000):
            key = randint(0, TEST_INT_RANGE_UPPER)
            if key in self.ref_dict:
                del self.ref_dict[key]
                del self.test_dict[key]
                self.assertEqual(len(self.ref_dict), len(self.test_dict))

        compare_int_dicts(self, self.ref_dict, self.test_dict, 0, TEST_INT_RANGE_UPPER)


class TestFloat(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Mdict()
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
            self.assertEqual(len(self.ref_dict), len(self.test_dict))

        compare_dicts(self, self.ref_dict, self.test_dict)


class TestBytes(unittest.TestCase):
    test_dict = None
    ref_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Mdict()
        cls.ref_dict = dict()

    def test_add_bytes(self):
        for i in range(10000):
            key = randbytes(10)
            value = randbytes(20)
            self.assertEqual(getrefcount(key), 2)
            self.assertEqual(getrefcount(value), 2)
            self.test_dict[key] = value
            # mdict does not increase ref_count
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
            self.assertEqual(len(self.ref_dict), len(self.test_dict))

        compare_dicts(self, self.ref_dict, self.test_dict)


class TestString(unittest.TestCase):
    test_dict = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dict = Mdict()

    def test_string(self):
        self.test_dict["Guangdong"] = "Shenzhen"
        self.test_dict["Sichuan"] = "Changsha"
        # overwrite
        self.test_dict["Sichuan"] = "Chengdu"
        self.test_dict["Beijing"] = "Beijing"
        del self.test_dict["Beijing"]

        # assertions
        assert "Beijing" not in self.test_dict
        assert self.test_dict["Sichuan"] == "Chengdu"
        assert self.test_dict["Guangdong"] == "Shenzhen"


if __name__ == '__main__':
    unittest.main()
